cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME tributary)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
#find_package(OpenSSL REQUIRED)
find_package(RdKafka REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
src/tributary_extension.cpp
src/tributary_scan_topic.cpp
src/tributary_config.cpp
src/tributary_exception.cpp
src/tributary_logging.cpp
src/query_farm_telemetry.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} RdKafka::rdkafka RdKafka::rdkafka++)
target_link_libraries(${LOADABLE_EXTENSION_NAME} RdKafka::rdkafka RdKafka::rdkafka++)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
