ADD_LIBRARY(planner OBJECT
        planner_utils.c
        predicate_management.c
        spatiotemporal_planner.c
        planner_strategies.c
        spatiotemporal_explain.c
        query_semantics_check.c
        join_processing.c
        )