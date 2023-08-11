// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Based on this example:
// https://github.com/google/sandboxed-api/blob/master/sandboxed_api/sandbox2/examples/static/static_sandbox.cc

#include <fcntl.h>
#include <glog/logging.h>
#include <linux/futex.h>
#include <linux/socket.h>
#include <netinet/tcp.h>
#include <sys/ioctl.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <syscall.h>
#include <unistd.h>

#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/internal/raw_logging.h"
#include "absl/memory/memory.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "sandboxed_api/sandbox2/executor.h"
#include "sandboxed_api/sandbox2/limits.h"
#include "sandboxed_api/sandbox2/policy.h"
#include "sandboxed_api/sandbox2/policybuilder.h"
#include "sandboxed_api/sandbox2/result.h"
#include "sandboxed_api/sandbox2/sandbox2.h"
#include "sandboxed_api/sandbox2/util.h"
#include "sandboxed_api/sandbox2/util/bpf_helper.h"
#include "sandboxed_api/util/flag.h"
#include "sandboxed_api/util/runfiles.h"
#include "tools/cpp/runfiles/runfiles.h"

namespace fs = std::filesystem;

std::unique_ptr<sandbox2::Policy> GetPolicy(std::string nodePath) {
    return sandbox2::PolicyBuilder()
        // Syscall table for x86_64:
        // https://blog.rchapman.org/posts/Linux_System_Call_Table_for_x86_64/
        // Some system policies described here are preceeded by "[syscall
        // number/syscall name], reason"

        // To run a binary, the libraries for it need to be allowed.
        .AddLibrariesForBinary(nodePath)

        // System folder and files.
        .AddFile("/dev/urandom", false)
        .AddFile("/etc/localtime")
        // Proc needed for retrieving fd. TODO: Make more specific.
        .AddDirectory("/proc", false)
        .AddFile("/dev/null", false)

        // Futex:
        // [202/futex], fast user-space locking, used by v8 when available (if not
        // it emulates them), which Node depends on..AllowFutexOp(FUTEX_WAKE)
        .AllowFutexOp(FUTEX_WAKE)
        .AllowFutexOp(FUTEX_WAIT)
        .AllowFutexOp(FUTEX_CMP_REQUEUE)

        // Signals:
        // [14/rt_sigprocmask], examine and change blocked signals, used by Node)
        // Note that only SIG_SETMASK is set. This is used for things like error
        // stream handling.
        // https://github.com/nodejs/node/blob/e2793049542b50bcbb07148cd17ed7c517faa467/src/node.cc#L589.
        .AddPolicyOnSyscall(__NR_rt_sigprocmask,
                            {
                                ARG_32(0),
                                JEQ32(SIG_SETMASK, ALLOW),
                            })
        // TODO: Remove this and limit [13/rt_sigation] directly? If not, remove
        // rt_sigprocmask, as this is covered by allowing all signals here.
        .AllowHandleSignals()

        // File descriptors:
        .AllowWrite()
        .AllowRead()
        .AllowReaddir()
        // [72/fcntl], manipulate file descriptor.
        // Used by node for dealing with stdio streams:
        // https://github.com/nodejs/node/blob/e2793049542b50bcbb07148cd17ed7c517faa467/src/node.cc#L628
        .AllowSafeFcntl()
        // [332/statx], get file status
        .AllowSyscall(__NR_statx)
        // [290/eventfd2], create a file descriptor for event notification.
        .AllowSyscall(__NR_eventfd2)
        .AllowSyscall(__NR_ioctl)

        // Namespaced file system access (we need to read files for compilation):
        // [257/openat], open a file relative to a directory file descriptor.
        // Required for opening files.
        .AllowOpen()
        // [9/mmap], map or unmap files or devices into memory.
        // JS files are loaded into memory by Node.
        .AllowMmap()
        // [21/access], read info on files.
        .AllowAccess()
        // [79/getcwd], get current working directory

        // Sockets, pipes and IPC
        .AllowSyscall(__NR_getsockopt)
        .AllowSyscall(__NR_sendto)
        .AllowSyscall(__NR_recvfrom)
        .AllowSyscall(__NR_shutdown)
        .AllowSyscall(__NR_bind)
        .AllowSyscall(__NR_listen)
        .AllowSyscall(__NR_connect)
        .AllowSyscall(__NR_getsockname)
        .AllowSyscall(__NR_getpeername)
        // Allow epoll I/O event notification and piping for fd data transferral.
        .AllowSyscalls(
            {__NR_epoll_create1, __NR_pipe2, __NR_epoll_ctl, __NR_epoll_pwait})

        // Processes:
        // Various syscalls for getting user and group IDs.
        .AllowGetIDs()
        .AllowSyscalls({__NR_getpid, __NR_gettid})
        // Node uses worker threads.
        .AllowFork()
        // Setting PR limit is used for managing these worker threads.
        .AllowSyscall(__NR_prlimit64)
        // [234/tgkill], send a signal to a thread.
        .AllowSyscall(__NR_tgkill)
        // TODO: Why deny this, with EPERM?
        .BlockSyscallWithErrno(__NR_prlimit64, EPERM)

        // Memory management.
        // [28/madvise], give advice about use of memory
        .AllowSyscall(__NR_madvise)

        // Misc:
        // [318/getrandom], obtain a series of random bytes
        .AllowExit()
        .AllowGetRandom()
        .AllowDynamicStartup()

        // Temporary, for development.
        .AddFile("/usr/local/google/home/eliaskassell/tmp.js")

        // Aggressively fail if policy builder fails.
        .BuildOrDie();
}

void OutputFD(int stdoutFd, int errFd) {
    for (;;) {
        char stdoutBuf[4096];
        char stderrBuf[4096];
        ssize_t stdoutRLen = read(stdoutFd, stdoutBuf, sizeof(stdoutBuf));
        ssize_t stderrRLen = read(errFd, stderrBuf, sizeof(stderrBuf));
        printf("stdout: '%s'\n", std::string(stdoutBuf, stdoutRLen).c_str());
        printf("stderr: '%s'\n", std::string(stderrBuf, stderrRLen).c_str());
        if (stdoutRLen < 1) {
            break;
        }
    }
}

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    // // Useful for debugging finding the worker bundle
    // std::cout << "Current path is " << fs::current_path() << '\n';
    // std::string path = "./sandbox";
    // for (const auto& entry : fs::directory_iterator(path))
    //     std::cout << entry.path() << std::endl;

    std::string workspaceFolder = "df/";

    std::string nodeRelativePath(argv[1]);
    std::string nodePath =
        sapi::GetDataDependencyFilePath(workspaceFolder + nodeRelativePath);

    std::string compilePath = sapi::GetDataDependencyFilePath("./sandbox/worker_bundle.js");
    // TODO: Should bazel location be passed in instead?
    // std::string compileRelativePath(argv[2]);
    // std::string compilePath =
    //     sapi::GetDataDependencyFilePath(workspaceFolder + compileRelativePath);

    std::vector<std::string> args = {
        nodePath,
        compilePath,
    };
    printf("Running command: '%s'\n", (args[0] + " " + args[1]).c_str());

    auto executor = absl::make_unique<sandbox2::Executor>(nodePath, args);

    executor->set_enable_sandbox_before_exec(true)
        .limits()
        // Remove restrictions on the size of address-space of sandboxed
        // processes.
        ->set_rlimit_as(RLIM64_INFINITY);

    int stdoutFd = executor->ipc()->ReceiveFd(STDOUT_FILENO);
    int stderrFd = executor->ipc()->ReceiveFd(STDERR_FILENO);

    auto policy = GetPolicy(nodePath);
    sandbox2::Sandbox2 s2(std::move(executor), std::move(policy));

    sandbox2::Result result;
    if (s2.RunAsync()) {
        OutputFD(stdoutFd, stderrFd);
        result = s2.AwaitResult();
    } else {
        printf("Sandbox failed\n");
    }

    printf("Final execution status: %s\n", result.ToString().c_str());

    return result.final_status() == sandbox2::Result::OK ? EXIT_SUCCESS
                                                         : EXIT_FAILURE;
}
