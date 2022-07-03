#include "io_uring.h"

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <fcntl.h>

#include "gtest/gtest.h"

namespace leveldb {

TEST(io_uring, ReadWriteFile) {
  // int fd = open("/tmp/io_uring_test", O_CREAT | O_RDWR, 0644);
  int fd = ::open("./io_uring_test", O_CREAT | O_RDWR | O_DIRECT, 0644);
  ASSERT_GT(fd, 0);
  // fallocate(fd, 0, 0, 8192);
  assert(fd > 0);
  int ret;
  uint8_t* buf1 = nullptr;
  ret = posix_memalign(reinterpret_cast<void**>(&buf1), 4096, 4096);
  ASSERT_EQ(ret, 0);
  memset(buf1, '1', 4096);
  buf1[4096 - 1] = '\n';
  uint8_t* buf2 = nullptr;
  ret = posix_memalign(reinterpret_cast<void**>(&buf2), 4096, 4096);
  ASSERT_EQ(ret, 0);
  memset(buf2, '2', 4096);
  buf2[4096 - 1] = '\n';
  {
    auto handle1 =
        IoUring::Instance().DoIoReq(IoReq(true, fd, 4096, 8192, buf1));
    auto handle2 =
        IoUring::Instance().DoIoReq(IoReq(true, fd, 4096, 12288, buf2));
    // start time
    auto start = std::chrono::system_clock::now();
    handle1->Wait();
    handle2->Wait();
    // end time
    auto end = std::chrono::system_clock::now();
    // print double seconds
    std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                       start)
                     .count()
              << " ms" << std::endl;
  }
  memset(buf1, 0, 4096);
  memset(buf2, 0, 4096);
  {
    auto handle1 =
        IoUring::Instance().DoIoReq(IoReq(false, fd, 4096, 8192, buf1));
    auto handle2 =
        IoUring::Instance().DoIoReq(IoReq(false, fd, 4096, 12288, buf2));
    // start time
    auto start = std::chrono::system_clock::now();
    handle1->Wait();
    handle2->Wait();
    // end time
    auto end = std::chrono::system_clock::now();
    // print double seconds
    std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                       start)
                     .count()
              << " ms" << std::endl;
  }
  ::close(fd);
  for (int i = 0; i < 4095; i++) {
    EXPECT_EQ(buf1[i], '1');
    EXPECT_EQ(buf2[i], '2');
  }
  EXPECT_EQ(buf1[4095], '\n');
  EXPECT_EQ(buf2[4095], '\n');
  delete buf1;
  delete buf2;
}

}  // namespace leveldb
