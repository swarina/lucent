cmake_minimum_required(VERSION 3.25)

project(lucent
  VERSION 0.0.1
  DESCRIPTION "A distributed vector search engine you can watch think"
  LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "" FORCE)
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Warnings-as-a-baseline for our own targets (see cpp/cmake helper).
add_library(lucent_warnings INTERFACE)
target_compile_options(lucent_warnings INTERFACE
  -Wall -Wextra -Wpedantic -Wconversion -Wshadow)

# Sanitizer toggle: -DLUCENT_SANITIZE=address|undefined|thread (CI uses this).
if(LUCENT_SANITIZE)
  add_compile_options(-fsanitize=${LUCENT_SANITIZE} -fno-omit-frame-pointer -g)
  add_link_options(-fsanitize=${LUCENT_SANITIZE})
endif()

enable_testing()
add_subdirectory(cpp)
