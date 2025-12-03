cmake_minimum_required(VERSION 3.15)
project(art C CXX)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Build type
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# Compiler flags
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra")
set(CMAKE_C_FLAGS_DEBUG "-g -O0")
set(CMAKE_C_FLAGS_RELEASE "-O3 -DNDEBUG")

# ============================================================================
# Dependencies
# ============================================================================

# Find cJSON library
find_package(PkgConfig)
if(PkgConfig_FOUND)
    pkg_check_modules(CJSON libcjson)
endif()

# If not found via pkg-config, try find_library
if(NOT CJSON_FOUND)
    find_library(CJSON_LIBRARY NAMES cjson)
    find_path(CJSON_INCLUDE_DIR NAMES cjson/cJSON.h)
    
    if(CJSON_LIBRARY AND CJSON_INCLUDE_DIR)
        set(CJSON_FOUND TRUE)
        set(CJSON_LIBRARIES ${CJSON_LIBRARY})
        set(CJSON_INCLUDE_DIRS ${CJSON_INCLUDE_DIR})
    endif()
endif()

if(NOT CJSON_FOUND)
    message(STATUS "cJSON not found. Install with: sudo apt-get install libcjson-dev")
    message(STATUS "Or on macOS: brew install cjson")
    message(FATAL_ERROR "cJSON library is required but not found")
endif()

message(STATUS "Found cJSON: ${CJSON_LIBRARIES}")

# ============================================================================
# Common Library
# ============================================================================

file(GLOB_RECURSE COMMON_SOURCES "common/src/*.c")
if(COMMON_SOURCES)
    add_library(art_common STATIC ${COMMON_SOURCES})
    target_include_directories(art_common PUBLIC common/include)
else()
    # Create interface library if no sources
    add_library(art_common INTERFACE)
    target_include_directories(art_common INTERFACE common/include)
endif()

# ============================================================================
# Database Library
# ============================================================================

file(GLOB DATABASE_SOURCES "database/src/*.c")
add_library(art_database STATIC ${DATABASE_SOURCES})
target_include_directories(art_database PUBLIC 
    database/include
)
target_link_libraries(art_database PUBLIC 
    art_common
)

# ============================================================================
# State Library
# ============================================================================

file(GLOB_RECURSE STATE_SOURCES "state/src/*.c")
if(STATE_SOURCES)
    add_library(art_state STATIC ${STATE_SOURCES})
    target_include_directories(art_state PUBLIC state/include)
    target_link_libraries(art_state PUBLIC 
        art_database
        art_common
    )
endif()

# ============================================================================
# EVM Library
# ============================================================================

file(GLOB_RECURSE EVM_SOURCES "evm/src/*.c")
if(EVM_SOURCES)
    add_library(art_evm STATIC ${EVM_SOURCES})
    target_include_directories(art_evm PUBLIC evm/include ${CMAKE_SOURCE_DIR}/third_party)
    target_link_libraries(art_evm PUBLIC 
        art_state
        art_common
    )
endif()

# ============================================================================
# Integration Tests Library
# ============================================================================

file(GLOB_RECURSE INTEGRATION_TEST_SOURCES "integration_tests/src/*.c")
if(INTEGRATION_TEST_SOURCES)
    add_library(art_integration_tests STATIC ${INTEGRATION_TEST_SOURCES})
    target_include_directories(art_integration_tests PUBLIC 
        integration_tests/include
        ${CJSON_INCLUDE_DIRS}
    )
    target_link_libraries(art_integration_tests PUBLIC 
        art_state
        art_evm
        art_common
        ${CJSON_LIBRARIES}
    )
endif()

# ============================================================================
# Tests (optional)
# ============================================================================

option(BUILD_TESTS "Build tests" ON)
if(BUILD_TESTS)
    enable_testing()
    
    # Common tests
    file(GLOB COMMON_TEST_SOURCES "common/tests/*.c")
    if(COMMON_TEST_SOURCES)
        foreach(test_file ${COMMON_TEST_SOURCES})
            get_filename_component(test_name ${test_file} NAME_WE)
            add_executable(${test_name} ${test_file})
            target_link_libraries(${test_name} art_common)
            add_test(NAME ${test_name} COMMAND ${test_name})
        endforeach()
    endif()
    
    # Database tests
    file(GLOB DATABASE_TEST_SOURCES "database/tests/*.c")
    if(DATABASE_TEST_SOURCES)
        foreach(test_file ${DATABASE_TEST_SOURCES})
            get_filename_component(test_name ${test_file} NAME_WE)
            add_executable(${test_name} ${test_file})
            target_link_libraries(${test_name} art_database)
            add_test(NAME ${test_name} COMMAND ${test_name})
        endforeach()
    endif()
    
    # State tests
    file(GLOB STATE_TEST_SOURCES "state/tests/*.c")
    if(STATE_TEST_SOURCES)
        foreach(test_file ${STATE_TEST_SOURCES})
            get_filename_component(test_name ${test_file} NAME_WE)
            add_executable(${test_name} ${test_file})
            target_link_libraries(${test_name} art_state)
            
            # Link cJSON for tests that need JSON parsing
            if(test_name STREQUAL "test_mpt_vectors")
                target_include_directories(${test_name} PRIVATE ${CJSON_INCLUDE_DIRS})
                target_link_libraries(${test_name} ${CJSON_LIBRARIES})
            endif()
            
            add_test(NAME ${test_name} COMMAND ${test_name})
        endforeach()
    endif()
    
    # EVM tests
    file(GLOB EVM_TEST_SOURCES "evm/tests/*.c")
    if(EVM_TEST_SOURCES)
        foreach(test_file ${EVM_TEST_SOURCES})
            get_filename_component(test_name ${test_file} NAME_WE)
            add_executable(${test_name} ${test_file})
            target_link_libraries(${test_name} art_evm)
            add_test(NAME ${test_name} COMMAND ${test_name})
        endforeach()
    endif()
    
    # Integration test parser verification
    if(TARGET art_integration_tests)
        add_executable(test_parser_verify integration_tests/test_parser_verify.c)
        target_link_libraries(test_parser_verify art_integration_tests)
        # Note: This is a manual test, not added to CTest
        
        add_executable(test_runner_example integration_tests/test_runner_example.c)
        target_link_libraries(test_runner_example art_integration_tests)
        # Note: This is a manual test, not added to CTest
        
        add_executable(test_runner_batch integration_tests/test_runner_batch_main.c)
        target_link_libraries(test_runner_batch art_integration_tests)
        # Note: This is the main integration test runner
    endif()
endif()

# ============================================================================
# Installation
# ============================================================================

set(INSTALL_TARGETS art_common art_database)
if(TARGET art_state)
    list(APPEND INSTALL_TARGETS art_state)
endif()
if(TARGET art_evm)
    list(APPEND INSTALL_TARGETS art_evm)
endif()

install(TARGETS ${INSTALL_TARGETS}
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
)

install(DIRECTORY 
    common/include/
    database/include/
    state/include/
    evm/include/
    DESTINATION include/art
)
