# issue_scylladb
This repository contains a sequence of operation that illustrates a problem encountered with ScyllaDb

The code is compiled in the following way:
```sh
$ cargo build
```

The test can then be run by
```sh
$ ./target/debug/test_scylladb RES
```

There are exactly 931 batches in the file RES, theyb are processed one by one. Then after being
submitted the keys are read and then check if they match with the expected state.

