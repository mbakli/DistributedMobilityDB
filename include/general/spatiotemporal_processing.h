#ifndef SPATIOTEMPORAL_PROCESSING_H
#define SPATIOTEMPORAL_PROCESSING_H
#include "postgres.h"

#define S_BOX_PTR(c)  ( (GBOX *) DatumGetPointer(c) )
#define DATUM_GET_SBOX(c)  ( S_BOX_PTR(c) )
#define BOX_GET_DATUM(c)  ( PointerGetDatum(c) )
#define ST_BOX_PTR(c)  ( (STBOX *) DatumGetPointer(c) )
#define DATUM_GET_STBOX(c)  ( ST_BOX_PTR(c) )
#endif /* SPATIOTEMPORAL_PROCESSING_H */
