add_subdirectory("helper_functions")
add_subdirectory("partitioning")

# Add the main sql file to the temporary file
configure_file(distributed_mobilitydb--1.0.sql ${CMAKE_CURRENT_BINARY_DIR}/${DISTRIBUTED_MOBILITYDB_EXTENSION_FILE}.in)

list(SORT PROJECT_SQL_FILES)
#######################################################
# Read all SQL files and add them to the .in.sql file
#######################################################
add_custom_target(all_sql_files ALL
        DEPENDS ${PROJECT_SQL_FILES})

foreach (f ${PROJECT_SQL_FILES})
    file(READ ${f} CURR_CONTENTS)
    file(APPEND ${CMAKE_CURRENT_BINARY_DIR}/${DISTRIBUTED_MOBILITYDB_EXTENSION_FILE}.in "${CURR_CONTENTS}")
endforeach()

# Copy the temporary file to the final location
configure_file(${CMAKE_CURRENT_BINARY_DIR}/${DISTRIBUTED_MOBILITYDB_EXTENSION_FILE}.in ${CMAKE_BINARY_DIR}/${DISTRIBUTED_MOBILITYDB_EXTENSION_FILE} COPYONLY)
