#ifndef POST_PROCESSING_H
#define POST_PROCESSING_H



typedef struct PostProcessing
{
    List *functions; // DistributedFunction;
    bool group_by_required;

} PostProcessing;

#endif /* POST_PROCESSING_H */
