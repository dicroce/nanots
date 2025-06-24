
#include "framework.h"
#include <stdarg.h>
#include <stdlib.h>
#include <time.h>

#ifdef _WIN32
#include <Windows.h>
#include <io.h>
#include <stdio.h>
#include <strsafe.h>
#include <tchar.h>
#else
#include <dirent.h>
#include <sys/time.h>
#include <unistd.h>

#endif

using namespace std;

vector<shared_ptr<test_fixture>> _test_fixtures;

#ifdef _WIN32
int64_t GetSystemTimeAsUnixTime() {
  // Get the number of seconds since January 1, 1970 12:00am UTC
  // Code released into public domain; no attribution required.

  const int64_t UNIX_TIME_START =
      0x019DB1DED53E8000;  // January 1, 1970 (start of Unix epoch) in "ticks"
  const int64_t TICKS_PER_SECOND = 10000000;  // a tick is 100ns

  FILETIME ft;
  GetSystemTimeAsFileTime(&ft);  // returns ticks in UTC

  // Copy the low and high parts of FILETIME into a LARGE_INTEGER
  // This is so we can access the full 64-bits as an int64_t without causing an
  // alignment fault
  LARGE_INTEGER li;
  li.LowPart = ft.dwLowDateTime;
  li.HighPart = ft.dwHighDateTime;

  // Convert ticks since 1/1/1970 into seconds
  return (li.QuadPart - UNIX_TIME_START) / TICKS_PER_SECOND;
}
#endif

void rtf_usleep(unsigned int usec) {
#ifdef _WIN32
  Sleep(usec / 1000);
#else
  usleep(usec);
#endif
}

string rtf_format(const char* fmt, ...) {
  va_list args;
  va_start(args, fmt);
  const string result = rtf_format(fmt, args);
  va_end(args);
  return result;
}

string rtf_format(const char* fmt, va_list& args) {
  va_list newargs;
  va_copy(newargs, args);
  const int chars_written = vsnprintf(nullptr, 0, fmt, newargs);
  const int len = chars_written + 1;

  vector<char> str(len);

  va_end(newargs);

  va_copy(newargs, args);

  vsnprintf(&str[0], len, fmt, newargs);

  va_end(newargs);

  string formatted(&str[0]);

  return formatted;
}

void rtf_remove_file(const std::string& fileName) {
#ifdef _WIN32
  // Windows implementation
  if (!DeleteFileA(fileName.c_str())) {
    // Handle error if needed
    // GetLastError() can be used to get error details
  }
#else
  // Linux/Unix implementation
  if (unlink(fileName.c_str()) != 0) {
    // Handle error if needed
    // errno contains error details
  }
#endif
}

bool rtf_file_exists(const std::string& fileName) {
#ifdef _WIN32
  return (_access(fileName.c_str(), 0) == 0);
#else
  return (access(fileName.c_str(), 0) == 0);
#endif
}

// This is a globally (across test) incrementing counter so that tests can avoid
// having hardcoded port numbers but can avoid stepping on eachothers ports.
int _next_port = 5000;

int rtf_next_port() {
  int ret = _next_port;
  _next_port++;
  return ret;
}

void handle_terminate() {
  printf("\nuncaught exception terminate handler called!\n");
  fflush(stdout);

  std::exception_ptr p = std::current_exception();

  if (p) {
    try {
      std::rethrow_exception(p);
    } catch (std::exception& ex) {
      printf("caught an exception in custom terminate handler: %s, %s:%d\n",
             ex.what(), __FILE__, __LINE__);
    } catch (...) {
      printf("caught an unknown exception in custom terminate handler.\n");
    }
  }
}

bool rtf_ends_with(const string& a, const string& b) {
  if (b.size() > a.size())
    return false;
  return std::equal(a.begin() + a.size() - b.size(), a.end(), b.begin());
}

vector<string> rtf_regular_files_in_dir(const string& dir) {
  vector<string> names;

#ifdef _WIN32
  WIN32_FIND_DATA ffd;
  TCHAR szDir[1024];
  HANDLE hFind;

  StringCchCopyA(szDir, 1024, dir.c_str());
  StringCchCatA(szDir, 1024, "\\*");

  // Find the first file in the directory.

  hFind = FindFirstFileA(szDir, &ffd);

  if (INVALID_HANDLE_VALUE == hFind)
    throw std::runtime_error("Unable to open directory");

  do {
    if (!(ffd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY))
      names.push_back(string(ffd.cFileName));
  } while (FindNextFileA(hFind, &ffd) != 0);

  FindClose(hFind);
#else
  DIR* d = opendir(dir.c_str());
  if (!d)
    throw std::runtime_error("Unable to open directory");

  struct dirent* e = readdir(d);

  if (e) {
    do {
      string name(e->d_name);
      if (e->d_type == DT_REG && name != "." && name != "..")
        names.push_back(name);
      e = readdir(d);
    } while (e);
  }

  closedir(d);
#endif

  return names;
}

int main(int argc, char* argv[]) {
  set_terminate(handle_terminate);

  std::string fixture_name = "";
  std::string test_name = "";
  std::string full_test_name = "";

  // Parse command line arguments
  if (argc > 1) {
    std::string arg1 = argv[1];
    
    // Check if it's in "fixture::test" format
    size_t pos = arg1.find("::");
    if (pos != std::string::npos) {
      fixture_name = arg1.substr(0, pos);
      test_name = arg1.substr(pos + 2);
      // The test names in the framework are stored as "fixture::method"
      full_test_name = arg1;
    } else {
      fixture_name = arg1;
      // Check for separate test name argument
      if (argc > 2) {
        test_name = argv[2];
        full_test_name = fixture_name + "::" + test_name;
      }
    }
  }

  // Print usage info if both fixture and test specified
  if (!fixture_name.empty() && !test_name.empty()) {
    printf("Running specific test: %s::%s\n", fixture_name.c_str(), test_name.c_str());
  } else if (!fixture_name.empty()) {
    printf("Running fixture: %s\n", fixture_name.c_str());
  } else {
    printf("Running all tests\n");
  }

#ifdef _WIN32
  srand((unsigned int)GetSystemTimeAsUnixTime());
#else
  srand(time(0));
#endif

  bool something_failed = false;
  int total_tests_run = 0;

  for (auto& tf : _test_fixtures) {
    if (!fixture_name.empty())
      if (tf->get_name() != fixture_name)
        continue;

    // Pass the full test name for specific test execution
    int tests_run = tf->run_tests(full_test_name);
    total_tests_run += tests_run;

    if (tf->something_failed()) {
      something_failed = true;
      tf->print_failures();
    }
  }

  // Only print Success/Failure if at least one test was run
  if (total_tests_run > 0) {
    if (!something_failed)
      printf("\nSuccess.\n");
    else
      printf("\nFailure.\n");
  } else {
    printf("\nNo tests were run.\n");
    // Exit with error code if a specific test was requested but not found
    if (!fixture_name.empty() || !test_name.empty()) {
      printf("Error: Requested test not found.\n");
      return 1;
    }
  }

  if (something_failed)
    if (system("/bin/bash -c 'read -p \"Press Any Key\"'") < 0) {
      printf("system() failure.\n");
    }

  return 0;
}
