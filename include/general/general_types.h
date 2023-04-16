#ifndef GENERAL_TYPES_H
#define GENERAL_TYPES_H

/* Shape Type */
typedef enum ShapeType
{
    SPATIAL,
    SPATIOTEMPORAL,
    DIFFTYPE
} ShapeType;

#define Var_Schema "dist_mobilitydb"
#define Var_Spatiotemporal_Index "GIST"
#define Var_BTREE_Index "BTREE"


#endif /* GENERAL_TYPES_H */
