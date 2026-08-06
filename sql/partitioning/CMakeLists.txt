SET(LOCAL_FILES
        preprocessing.sql
        shape_segmentation.sql
        search_in_dimensions.sql
        crange.sql
        hierarchical.sql
        period.sql
        quadtree.sql
        colocation.sql
        tiling.sql
        data_allocation.sql
        )
# parallel_data_allocation.sql is deliberately NOT built into the extension yet -- see the
# EXPERIMENTAL warning at the top of that file. It reproducibly triggered a Citus backend
# SIGSEGV during isolated testing (root cause not fully resolved); add it back to LOCAL_FILES
# above once that's understood and it's been proven safe against a real cluster.

foreach (f ${LOCAL_FILES})
    configure_file(${f} ${f})
    list(APPEND PACKAGE_SQL_FILES ${CMAKE_CURRENT_BINARY_DIR}/${f})
endforeach()

set(PROJECT_SQL_FILES ${PROJECT_SQL_FILES} ${PACKAGE_SQL_FILES} PARENT_SCOPE)

