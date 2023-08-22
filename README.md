# s3lock

# 1. Understanding the problem

## File looks in Linux

### Mandatory locks
- Kernel maintains **data structures** for keeping track of file locks. This typically involves inode structures, which represent files.
- Processes communicate their intention to lock or unlock a file to the kernel using **system calls**. Done with "mand" command.

Lock types:
- **Read lock**: Multiple processes can hold read locks on a file simultaneously, but a write lock will be blocked until all read locks are released.
- **Write lock**: If a process holds a write lock on a file, no other process can acquire a read or write lock on that file until the original lock is released.

What happends if a process tries to lock a file that is already locked:
1. The process is put to sleep by the kernel.
2. When a file is unlocked the kernel wakes up one or more of the sleeping processes.

Queue system for processes waiting to lock and write/read a file?

## AWS S3

Data is is stored as **Objects** in **buckets**.

**objects**
Each **object typically** includes:
- Data.
- Globally unique identifier.
- Metadata.

Objects are immutable. They must be overwritten if you want to change them.

**Buckets**:
- Equivalent of a folder.
- Nested buckets not supported.
- Globally unique names within AWS S3 ecosystem.

How does S3 handle concurrent write and reads?


# 2. Setting up environment

1. Created AWS account.
2. Created an S3 bucket.
3. Created an IAM user with full permissions pertaining to S3.
4. Installed aws-cli. Configured aws-cli with access key, secret access key and region.
5. Installed go, aws-toolkit and draw.io extensions.

Note: I created the user and S3 bucket directly in the aws management console, but after looking into the aws-toolkit extension I could as well have done it directly in VS code.


# 3. Designing the library

## Acquiring a lock
If the lock instance already has the lock: 
1. Increment local lockCount.

If the lock instance does not have the lock:
1. Try to create the lockfile only if it does not exist (use PutObject with input.SetIfNoneMatch("*")).
2. If above operation failed. Check if the current lock has timed out. If it has, try to acquire it (see handling timeouts).

Question: After incrementing the total lockCount, should the file be updated, to reset the timeout? This could potentially block the lock if a user of the library code implements the code wrong.

## Releasing a lock
1. Decrement local lockCount.
2. If lockCount = 0, delete the lockfile in S3.

Potential problem: What if the operation took longer than the lock timeout and another process acquired the lock? It could remove the lock from another process.

Solution: Check that the process still owns the lock in S3.

## Handling timeouts 
1. Get the object from the bucket and check if it has timed out.
2. If the object has timed out, save the Etag.
3. Make a conditional rewrite of the object. Condition: Etags match. This to make sure that the object has not been changed between the API calls, ensuring atomicity.

## Handling re-entrancy
- Each lock instance is competing for the lock.
- Attributes: lockAcquired bool, lockCount int, guid int.

## Logging and troubleshooting
- Keep guid as metadata.
- Keep guid as attribute in lock.
- Log when: acquiring, releasing.

## Settings
- Keep bucketName and lockObjectName in seperate settings file for global settings.

## Additional thoughts 
Implementing this in Dynamodb would be far less cumbersome since it supports conditional writes to a higher degree. Meaning it would be a lot easier to maintain atomicity. I also beleive it would result in less and more readable code; making it more maintainable.


# 4. Challanges along the way.

## Atomicity
It turns out that there is no way at all to do conditional writes in sdk version2 (to my knowledge), which makes it hard to guarantee atomicity. The method I planned to use (use PutObject with input.SetIfNoneMatch("*")) seems to be decrepit. I spent a lot of time trying to find alternative ways of doing it, but did not find any.

I settled for doing it as good as I can with the tools I got and this is what I did:

E.g. Acquiring a lock:
1. Check if the lock is available. If it is available, move on to step 2.
2. Upload a file with with a metadata field of "lockowner", this is an attribute of the lock instance.
3. Sleep for a specified amount of time. Retreive metadata about the lockfile. If the uuid of the lock instance and the "lockowner" metadata fields match. Then the lock has been acquired.

This does not guarantee atomicity, but its the best I could think of without a conditional writes feature.

## Attempt to get atomicity with bucket versioning

### Research behaviour of operations with versioning enabled

Object versioning behaviour:

object_v1, object_v2, delete_marker_v3, object_v4 ...

Deleting objects:

**DeleteObject**: Can only delete one version of an object at a time.
**DeleteObjects**: Can specify a xml file with all versions to delete in one call. Gives back a list of failed deletes.


### Acquring a lock

If lock file does not exist:
1. Write the lockfile.

Check who owns a lock after write:
1. List all versions of an object.
2. Sort all versions based on "last modified".
3. Check the owner of the first version of the object. If this matches the with the owner of the lock instance --> lock acquired.

### Releasing a lock
1. Delete object --> this will add a "delete tag" as the next version.
2. List all versions of the object.
3. Sort all versions based on "last modified".
4. Check that the owner of the first "delete tag" is the same as the owner of the lock instance.
5. If there is a match -> delete all object versions.

Problem: Delete will fail if new versions are addded in between 2) and 5). E.g. two processes try to delete the object at the same time, each adding a delete tag.