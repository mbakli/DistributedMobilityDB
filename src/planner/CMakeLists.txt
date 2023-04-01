set(spatiotemporal_planner.c spatiotemporal_planner.c)


ADD_LIBRARY(planner OBJECT ${spatiotemporal_planner.c} spatiotemporal_explain.c query_semantics_check.c join_processing.c post_processing.c)