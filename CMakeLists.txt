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

#---------------------------------------------
# Postgresql Link
#---------------------------------------------
# Search paths for Postgres binaries

set(PG_MIN_MAJOR_VERSION "13")
set(PG_MAX_MAJOR_VERSION "14")

find_path(PostgreSQL_TYPE_INCLUDE_DIR
        NAMES catalog/pg_type.h
        PATHS
        # Look in other places.
        ${PostgreSQL_ROOT_DIRECTORIES}
        PATH_SUFFIXES
        postgresql
        pgsql/server
        postgresql/server
        include/server
        # Help the user find it if we cannot.
        DOC "The ${PostgreSQL_INCLUDE_DIR_MESSAGE}"
        )

if(POSTGRESQL_BIN)
    # Checking POSTGRESQL_PG_CONFIG
    find_program(POSTGRESQL_PG_CONFIG NAMES pg_config
            PATHS
            ${POSTGRESQL_BIN}
            NO_DEFAULT_PATH
            )
else()
    find_program(POSTGRESQL_PG_CONFIG NAMES pg_config)
endif()

if(POSTGRESQL_PG_CONFIG)
    execute_process(
            COMMAND ${POSTGRESQL_PG_CONFIG} --bindir
            OUTPUT_STRIP_TRAILING_WHITESPACE
            OUTPUT_VARIABLE T_POSTGRESQL_BIN)
endif(POSTGRESQL_PG_CONFIG)

# search for POSTGRESQL_EXECUTABLE _only_ in the dir specified by pg_config
find_program(POSTGRESQL_EXECUTABLE NAMES postgres
        PATHS
        ${T_POSTGRESQL_BIN}
        NO_DEFAULT_PATH
        )

if (PostgreSQL_INCLUDE_DIRS)
    message(STATUS "POSTGRES_INCLUDE_PATH ${PostgreSQL_INCLUDE_DIRS}")
else()
    find_package(PostgreSQL ${PG_MIN_MAJOR_VERSION} REQUIRED)
    set (PostgreSQL_INCLUDE_DIRS /usr/local/pgsql/include)
    message(STATUS "POSTGRES_INCLUDE_PATH ${PostgreSQL_INCLUDE_DIRS}")
    include_directories(SYSTEM ${PostgreSQL_INCLUDE_DIRS}/server)
endif()

include_directories(SYSTEM ${PostgreSQL_INCLUDE_DIRS})

#---------------------------------------------
# library directory
#---------------------------------------------
execute_process(
COMMAND ${POSTGRESQL_PG_CONFIG} --pkglibdir
OUTPUT_STRIP_TRAILING_WHITESPACE
OUTPUT_VARIABLE LIBRARY_INSTALL_PATH)

if(NOT LIBRARY_INSTALL_PATH)
message(FATAL_ERROR "pg_config --pkglibdir failed to return a value. Please check your PostgreSQL installation!")
endif()

message(STATUS "LIBRARY_INSTALL_PATH ${LIBRARY_INSTALL_PATH}")

#---------------------------------------------
# extension directory
#---------------------------------------------

execute_process(
        COMMAND ${POSTGRESQL_PG_CONFIG} --sharedir
        OUTPUT_STRIP_TRAILING_WHITESPACE
        OUTPUT_VARIABLE SHARE_DIR)

#-------
if(SHARE_DIR)
    set(SHARE_DIR "${SHARE_DIR}/extension")
else(SHARE_DIR)
    message(FATAL_ERROR "pg_config --sharedir failed to return a value. Please check your PostgreSQL installation!")
endif(SHARE_DIR)


#include_directories(SYSTEM ${PostgreSQL_INCLUDE_DIRS}/server)

# For explain
include_directories(SYSTEM /usr/include/libsafec)
#-------------------
# add the postgis link
#-------------------
include(TestBigEndian)
TEST_BIG_ENDIAN(IS_BIG_ENDIAN)

include_directories(SYSTEM ${GEOS_INCLUDE_DIR})

# libpq-fe
if(PROJ_INCLUDE_DIRS)
    include_directories(SYSTEM ${PROJ_INCLUDE_DIRS})
    message(STATUS "POSTGIS_PROJ_VERSION: ${POSTGIS_PROJ_VERSION}")
endif()

#-------------------
# add the MobilityDB link
#-------------------
include_directories("/usr/local/include/meos")
#-------------------
# add the subdirectories that have the C/C++ code
#-------------------

include_directories("include")

add_subdirectory( "src")
add_subdirectory("sql")


#-------------------
# Distributed MobilityDB objects to be linked
#-------------------
set (DistMobilityDB_SOURCE_NAMES
        general
        catalog
        utils
        post_processing
        distributed_functions
        planner
        executor
        )

#-------------------
# Link Distributed MobilityDB Objects
#-------------------
foreach (subdir ${DistMobilityDB_SOURCE_NAMES} )
    set(PROJECT_OBJECTS ${PROJECT_OBJECTS} "$<TARGET_OBJECTS:${subdir}>")
endforeach()


set(POSTGRESQL_DYNLIB_DIR /usr/local/pgsql/lib)
set(POSTGRESQL_SHARE_DIR /usr/local/pgsql/share)

set(MOBILITYDB_LIB_NAME "${CMAKE_PROJECT_NAME}-1.0")
add_library(${MOBILITYDB_LIB_NAME} MODULE ${PROJECT_OBJECTS})


# Generate the .control and .sql files and put them in the build directory

add_custom_target(sqlscript ALL DEPENDS ${CMAKE_BINARY_DIR}/${DISTRIBUTED_MOBILITYDB_EXTENSION_FILE})
add_custom_target(control ALL DEPENDS ${CMAKE_BINARY_DIR}/distributed_mobilitydb.control)


install(TARGETS ${MOBILITYDB_LIB_NAME} DESTINATION "${POSTGRESQL_DYNLIB_DIR}")
install(FILES "${CMAKE_BINARY_DIR}/distributed_mobilitydb.control"
        "${CMAKE_BINARY_DIR}/${DISTRIBUTED_MOBILITYDB_EXTENSION_FILE}"
        DESTINATION "${POSTGRESQL_SHARE_DIR}/extension")
