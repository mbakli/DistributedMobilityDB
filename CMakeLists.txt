# Minimum version supporting fixtures
cmake_minimum_required(VERSION 3.7)

#project(DistributedMobilityDB C)
project(distributed_mobilitydb VERSION 1.0)

set(CMAKE_C_STANDARD 99)
set(DISTRIBUTED_MOBILITYDB_EXTENSION_FILE "distributed_mobilitydb--1.0.sql")
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")


include(CheckSymbolExists)

#-------------------------------------
# Set compiler flags
#-------------------------------------

include(CheckCCompilerFlag)
if(NOT WIN32)
    CHECK_C_COMPILER_FLAG("-fPIC" C_COMPILER_SUPPORTS_FPIC)
    if(C_COMPILER_SUPPORTS_FPIC)
        add_definitions(-fPIC)
    endif()
endif()

#-------------------
# add the postgresql link
#-------------------
# find_package(PostgreSQL REQUIRED)
include_directories(SYSTEM /usr/local/pgsql/include/server)
include_directories(SYSTEM /usr/local/pgsql/include)
# For explain
include_directories(SYSTEM /usr/include/libsafec)
# include_directories(SYSTEM ${POSTGRESQL_INCLUDE_DIR})

#-------------------
# add the postgis link
#-------------------
include(TestBigEndian)
TEST_BIG_ENDIAN(IS_BIG_ENDIAN)

include_directories(SYSTEM ${GEOS_INCLUDE_DIR})

# libpq-fe
# include_directories(SYSTEM ${PROJ_INCLUDE_DIRS})
# math(EXPR POSTGIS_PROJ_VERSION "${PROJ_VERSION_MAJOR} * 10 + ${PROJ_VERSION_MINOR}")
# message(STATUS "POSTGIS_PROJ_VERSION: ${POSTGIS_PROJ_VERSION}")


#-------------------
# add the subdirectories that have the C/C++ code
#-------------------
include_directories("include")
add_subdirectory( "src")
add_subdirectory("sql")



#set(PROJECT_OBJECTS "$<TARGET_OBJECTS:grid>" src/planning/planner.c include/planning/spatiotemporal_planner.h src/general/shared_library_init.c include/general/shared_library_init.h)
set(PROJECT_OBJECTS ${PROJECT_OBJECTS} "$<TARGET_OBJECTS:partitioning>" include/partitioning/tiling_utils.h include/partitioning/multirelation.h)
set(PROJECT_OBJECTS "$<TARGET_OBJECTS:general>")
set(PROJECT_OBJECTS ${PROJECT_OBJECTS} "$<TARGET_OBJECTS:executor>" include/executor/multi_phase_executor.h)
set(PROJECT_OBJECTS ${PROJECT_OBJECTS} "$<TARGET_OBJECTS:planner>" include/planner/predicate_management.h)
set(PROJECT_OBJECTS ${PROJECT_OBJECTS} "$<TARGET_OBJECTS:post_processing>" include/post_processing/post_processing.h)
set(PROJECT_OBJECTS ${PROJECT_OBJECTS} "$<TARGET_OBJECTS:distributed_functions>")

set(POSTGRESQL_DYNLIB_DIR /usr/local/pgsql/lib)
set(POSTGRESQL_SHARE_DIR /usr/local/pgsql/share)

set(MOBILITYDB_LIB_NAME "${CMAKE_PROJECT_NAME}-1.0")
add_library(${MOBILITYDB_LIB_NAME} MODULE ${PROJECT_OBJECTS})

# Generate the .control and .sql files and put them in the build directory
add_custom_target(sqlscript ALL DEPENDS ${CMAKE_BINARY_DIR}/${DISTRIBUTED_MOBILITYDB_EXTENSION_FILE})
add_custom_target(control ALL DEPENDS ${CMAKE_BINARY_DIR}/distributed_mobilitydb.control)


install(TARGETS ${MOBILITYDB_LIB_NAME} DESTINATION "${POSTGRESQL_DYNLIB_DIR}")
install(FILES "${CMAKE_BINARY_DIR}/distributed_mobilitydb.control"  "${CMAKE_BINARY_DIR}/${DISTRIBUTED_MOBILITYDB_EXTENSION_FILE}"
        DESTINATION "${POSTGRESQL_SHARE_DIR}/extension")