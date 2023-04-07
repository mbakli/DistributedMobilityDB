ADD_LIBRARY(planner OBJECT
        planner_utils.c
        spatiotemporal_planner.c
        spatiotemporal_explain.c
        query_semantics_check.c
        join_processing.c
        colocation.c
        noncolocation.c
        predicate_management.c
        tile_scan_rebalancer.c
        )