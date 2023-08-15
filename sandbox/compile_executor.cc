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

//std::string EXAMPLE_COMPILE_PATH = "/usr/local/google/home/eliaskassell/Documents/github/dataform/examples/common_v1";
std::string EXAMPLE_COMPILE_PATH = "/usr/local/google/home/lewishemens/workspace/dataform-data";
const int TIMEOUT_SECS = 10000;
const int FALLBACK_TIMEOUT_DELAY = 1000;


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
    printf("ALL ARGVS:\n");
    for(int i = 0; i < argc; i++) {
          printf("%i: %s\n", i, argv[i]);
    }

    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    std::string currentPath = std::string(fs::current_path()) + "/";

    std::string nodeRelativePath(argv[1]);
    std::string nodePath =
        sapi::GetDataDependencyFilePath(nodeRelativePath);
    std::string workerRelativeRoot(argv[2]);
    std::string workerRoot =  sapi::GetDataDependencyFilePath(workerRelativeRoot);

    // Useful for debugging paths.
    std::cout << "Current path is " << fs::current_path() << '\n';
    std::cout << "Worker path is " << workerRoot << '\n';


    std::vector<std::string> args = {
        nodePath,
        workerRoot + "/worker_bundle.js",
        // "-e",
        // "console.log('hello')"
    };
    printf("Running command: '%s'\n", (args[0] + " " + args[1]).c_str());
    // printf("Running command: '%s'\n", (args[0] + " " + args[1] + " " + args[2]).c_str());
    auto executor = absl::make_unique<sandbox2::Executor>(nodePath, args);

    executor->set_enable_sandbox_before_exec(true)
        .limits()
        ->set_rlimit_as(RLIM64_INFINITY)
        .set_rlimit_fsize(4ULL << 20)
        .set_rlimit_cpu(RLIM64_INFINITY)
        .set_walltime_limit(absl::Seconds(1000));

    int stdoutFd = executor->ipc()->ReceiveFd(STDOUT_FILENO);
    int stderrFd = executor->ipc()->ReceiveFd(STDERR_FILENO);
    // int dataformFd = executor->ipc()->ReceiveFd(3);

    auto policy = sandbox2::PolicyBuilder()
            // Workaround to make the forkserver's execveat work.
            .AddFileAt("/dev/zero", "/dev/fd/1022", false)

            .AddLibrariesForBinary(nodePath)

            // TODO: Make read only.
            .AddDirectory(workerRoot, true)

            .AddDirectory(EXAMPLE_COMPILE_PATH, false)
            .DangerDefaultAllowAll()
            .BuildOrDie();

    sandbox2::Sandbox2 s2(std::move(executor), std::move(policy));

    // If the sandbox program fails to start, return early.
    if (!s2.RunAsync()) {
        auto result = s2.AwaitResultWithTimeout(
            absl::Seconds(TIMEOUT_SECS + FALLBACK_TIMEOUT_DELAY));
        LOG(ERROR) << "sandbox failed to start: " << result->ToString();
        return EXIT_FAILURE;
    }

    auto result = s2.AwaitResultWithTimeout(
        absl::Seconds(TIMEOUT_SECS + FALLBACK_TIMEOUT_DELAY));

    OutputFD(stdoutFd, stderrFd);

    // Collect Dataform output FD.
    // char dataformBuf[20 * 1024 * 1024];
    // ssize_t dataformLen = 0;
    // for (;;) {
    //     ssize_t readLen = read(dataformFd, dataformBuf, sizeof(dataformBuf));
    //     dataformLen += readLen;
    //     if (readLen < 1) {
    //         break;
    //     }
    // }
    // printf("dataform: '%s'\n", std::string(dataformBuf, dataformLen).c_str());

    printf("Final execution status: %s\n", result->ToString().c_str());

    return result->final_status() == sandbox2::Result::OK ? EXIT_SUCCESS
                                                         : EXIT_FAILURE;
}
