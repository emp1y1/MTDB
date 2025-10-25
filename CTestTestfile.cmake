# CMake generated Testfile for 
# Source directory: /home/lxw/桌面/new2/new/leveldb
# Build directory: /home/lxw/桌面/new2/new/leveldb
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(leveldb_tests "/home/lxw/桌面/new2/new/leveldb/leveldb_tests")
set_tests_properties(leveldb_tests PROPERTIES  _BACKTRACE_TRIPLES "/home/lxw/桌面/new2/new/leveldb/CMakeLists.txt;361;add_test;/home/lxw/桌面/new2/new/leveldb/CMakeLists.txt;0;")
add_test(c_test "/home/lxw/桌面/new2/new/leveldb/c_test")
set_tests_properties(c_test PROPERTIES  _BACKTRACE_TRIPLES "/home/lxw/桌面/new2/new/leveldb/CMakeLists.txt;387;add_test;/home/lxw/桌面/new2/new/leveldb/CMakeLists.txt;390;leveldb_test;/home/lxw/桌面/new2/new/leveldb/CMakeLists.txt;0;")
add_test(env_posix_test "/home/lxw/桌面/new2/new/leveldb/env_posix_test")
set_tests_properties(env_posix_test PROPERTIES  _BACKTRACE_TRIPLES "/home/lxw/桌面/new2/new/leveldb/CMakeLists.txt;387;add_test;/home/lxw/桌面/new2/new/leveldb/CMakeLists.txt;398;leveldb_test;/home/lxw/桌面/new2/new/leveldb/CMakeLists.txt;0;")
subdirs("third_party/googletest")
subdirs("third_party/benchmark")
