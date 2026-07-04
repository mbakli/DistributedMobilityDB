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

add_definitions(-DPostgreSQL_TYPE_INCLUDE_DIR=/usr/include/postgresql/)

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


# Resolve include directories from the same pg_config used for --bindir above,
# instead of find_package(PostgreSQL) (which only locates the client libpq
# headers and can pick a different, unrelated PostgreSQL installation when
# several major versions are installed side by side).
execute_process(
        COMMAND ${POSTGRESQL_PG_CONFIG} --includedir-server
        OUTPUT_STRIP_TRAILING_WHITESPACE
        OUTPUT_VARIABLE PostgreSQL_SERVER_INCLUDE_DIR)
execute_process(
        COMMAND ${POSTGRESQL_PG_CONFIG} --includedir
        OUTPUT_STRIP_TRAILING_WHITESPACE
        OUTPUT_VARIABLE PostgreSQL_INCLUDE_DIR)

if (NOT PostgreSQL_SERVER_INCLUDE_DIR)
    message(FATAL_ERROR "pg_config --includedir-server failed to return a value. Please check your PostgreSQL installation!")
endif()

message(STATUS "POSTGRES_INCLUDE_PATH ${PostgreSQL_INCLUDE_DIR}")
message(STATUS "POSTGRES_SERVER_PATH ${PostgreSQL_SERVER_INCLUDE_DIR}")
include_directories(SYSTEM ${PostgreSQL_SERVER_INCLUDE_DIR})
include_directories(SYSTEM ${PostgreSQL_INCLUDE_DIR})

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

# liblwgeom.h (used for GBOX) is PostGIS' own internal header and is not
# shipped by any postgresql-*-postgis-3 package; it must be sourced from a
# PostGIS/MobilityDB checkout. See README for how to place a copy under
# /usr/local/include/postgis-lwgeom/liblwgeom (alongside its required
# ../postgis_config.h).
include_directories(SYSTEM /usr/local/include/postgis-lwgeom/liblwgeom)

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
        multirelation
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


# Install into the actual pg_config-reported directories for this PostgreSQL
# installation (LIBRARY_INSTALL_PATH/SHARE_DIR, computed above), rather than
# an author-specific /usr/local/pgsql source-build layout. Note SHARE_DIR
# already has "/extension" appended (see the extension-directory block above).
set(POSTGRESQL_DYNLIB_DIR ${LIBRARY_INSTALL_PATH})
set(POSTGRESQL_SHARE_DIR ${SHARE_DIR})

set(MOBILITYDB_LIB_NAME "${CMAKE_PROJECT_NAME}-1.0")
add_library(${MOBILITYDB_LIB_NAME} MODULE ${PROJECT_OBJECTS})


# Generate the .control and .sql files and put them in the build directory

add_custom_target(sqlscript ALL DEPENDS ${CMAKE_BINARY_DIR}/${DISTRIBUTED_MOBILITYDB_EXTENSION_FILE})
add_custom_target(control ALL DEPENDS ${CMAKE_BINARY_DIR}/distributed_mobilitydb.control)


install(TARGETS ${MOBILITYDB_LIB_NAME} DESTINATION "${POSTGRESQL_DYNLIB_DIR}")
install(FILES "${CMAKE_BINARY_DIR}/distributed_mobilitydb.control"
        "${CMAKE_BINARY_DIR}/${DISTRIBUTED_MOBILITYDB_EXTENSION_FILE}"
        DESTINATION "${POSTGRESQL_SHARE_DIR}")
