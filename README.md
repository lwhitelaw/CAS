# CAS
A content-addressable userspace filesystem and also one of my longest-running projects and proudest work.

Run this on the command-line to put in and take out files/folders based on their hashes. Objects are written to the `cas` folder in the current working directory. Rudimentary snapshot/commit functionality exists.

CAS was originally created as an odd experiment to "fuse" flashdrives together as one storage unit, but it also came out of needing a better solution than using Git for a purpose for which it wasn't designed for, especially in regards to "repositories" that were 10 gigabytes and larger. With enough work it had become reliable enough for me to use it as a backup solution. Unfortunately, while it works well, the UI lacks polish (who wants to enter and remember raw SHA3 hashes?) and it tried to be too "generic" to its detriment. In particular, it has functioning networking support (disabled in Main's getCAS()) and the integrated server supported multiple redundancy. However, it did somewhat solve Git's scaling issues by allowing in-place modifications of the storage repository without having to unpack everything and recommit.

This project is due for a rewrite from the ground up, with a better focus toward local and remote storage spaces like Git does; the project mostly failed in its original purpose to fuse drives together since it just was not as fast as reading and writing one drive in isolation.
