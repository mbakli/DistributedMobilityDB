SET(LOCAL_FILES
        preprocessing.sql
        shape_segmentation.sql
        search_in_dimensions.sql
        crange.sql
        colocation.sql
        tiling.sql
        data_allocation.sql
        )

foreach (f ${LOCAL_FILES})
    configure_file(${f} ${f})
    list(APPEND PACKAGE_SQL_FILES ${CMAKE_CURRENT_BINARY_DIR}/${f})
endforeach()

set(PROJECT_SQL_FILES ${PROJECT_SQL_FILES} ${PACKAGE_SQL_FILES} PARENT_SCOPE)

