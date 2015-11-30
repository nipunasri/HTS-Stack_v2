#define _OCL_CODE_
#include "HTSShared.hpp"

# pragma OPENCL EXTENSION cl_amd_printf:enable // enable printf on AMD GPU's


/************************* uiFree *****************************/
/* Function name	: uiFree
 * Arguments		: NodePool(pointer to nodepool), Node-index to be deleted	
 * Return value		: void
 * Description		: This function marks given index node as free node, by setting free bit,
					  executes at work-item level
 */
bool uiFree( TLLNode *pNodePool,	/* Hash-table + stack of free nodes */
			  uint uiHeadIndex,		/* pointer to free-node in pNodePool */
			  uint iDelNode )		/* node to be deleted */	
{ 
    uint uiLLMNode = 0 ; /* [<next-node-index>,<Bits(f,r,m)>] */
    uint uiLLNode = 0 ; /* next-node-index in nodepool, shift 3 bits right to get */ 
    bool bFoundNode =  false  ; /* true, node found otherwise not found */
    uint uiLLDelMNode = 0 ;
    uint uiLLNewNode = 0 ;
    uint freeBit = 0 ;
    
    uiLLDelMNode = pNodePool[iDelNode].uiNext ;
    freeBit = GET_FBIT(uiLLDelMNode) ;
    if(freeBit) return bFoundNode ;

    while( bFoundNode != true ) { /* iterate untill NodeFound */

        uiLLMNode = pNodePool[uiHeadIndex].uiNext ; /* Always top node is free : stack */
        uiLLNode = GET_PTR(uiLLMNode) ; /* Get actual index of next node in pool */
        uiLLDelMNode = SET_FBIT(uiLLMNode); /* index with free bit set */

        pNodePool[iDelNode].uiNext = uiLLDelMNode; // points to first node

        atomic_uint* pChgPtr =
            (atomic_uint *)(&(pNodePool[uiHeadIndex].uiNext));

        uiLLNewNode = SET_PTR(iDelNode) ;
        
        bFoundNode = atomic_compare_exchange_strong
            (pChgPtr,
            &uiLLMNode,
            uiLLNewNode);
        } // end-of-while-loop
    return bFoundNode ;
} // end-of-free

/************************* SNIP *****************************/
/* Function name : SNIP
 * Arguments	 : pNodePool, HeadIndex
 * Description   : This function takes starting index as input and deletes next marked node,
                   if it is marked for deletion otherwise return next-node-un-marked.
 * Return values : Returns snipped on successfull deletion of marked node, otherwise errors
 * Remarks		 : Executes at work-item level, meaning that only one work-item from work-group
                   executes this function at a time.
 */        
uint SNIP(TLLNode *pNodePool,	/* Hash-table + stack of free nodes */	
		  uint	   HeadIndex)	/* pointer to free-node in pNodePool */
{
    // private variables
    uint uiLLMNode = 0 ;/* 2:0->[f,r,m] and 31:3->next-node-index */
    uint uiLLNode = 0 ; /* next-node-index */
    uint pBits ; /* current-node-<f,r,m> bits, only 3 bits are using */;
    uint uiLLMNextNode = 0 ;/* 2:0->[f,r,m] and 31:3->next2next-node-index */
    uint uiLLNextNode = 0 ; /* next2next-node-index */
    uint nBits = 0 ; /* next-node-<f,r,m> bits, only 3 bits are using */;
    bool status = false ;

    uiLLMNode = pNodePool[HeadIndex].uiNext ; // next-node-index + <f,r,m>
    uiLLNode = GET_PTR(uiLLMNode) ; // next-node-index value, 31:3 bits
    pBits = GET_BITS(uiLLMNode) ; 
    if( (!IS_SET(pBits,2)) & (!IS_SET(pBits,1)) ) { // both retain,free has to be cleared
        uiLLMNextNode = pNodePool[uiLLNode].uiNext ; // 2:0->[f,r,m] and 31:3->next2next-node-index 
        uiLLNextNode = GET_PTR(uiLLMNextNode) ;
        nBits = GET_BITS(uiLLMNextNode) ; 
        if( (!IS_SET(nBits,2)) & (IS_SET(nBits,0)) ) { // <f,r,m> = <0,x,1>
            uiLLMNextNode = SET_PTR(uiLLNextNode) | pBits ; // make current-node next pointer as next2next node index
            atomic_uint* pChgPtr =
                (atomic_uint *)(&(pNodePool[HeadIndex].uiNext));
            status = atomic_compare_exchange_strong(pChgPtr, &uiLLMNode, uiLLMNextNode) ;
            if(status == true) {
                uiFree(pNodePool, HeadIndex, uiLLNode) ;
                return HTS_SNIP_SUCCESS ; /* successfully deleted next marked node */
            }
            return HTS_SNIP_FAILED ; /* failed in deleting */						
        }
        return HTS_NEXT_UNMARKED ; /* next-node is not marked */
    } 
    return HTS_INVALID_START ; /* provided node is not valid one */
} // end-of-SNIP

/************************* CLEAN *****************************/
/* Function name : CLEAN
 * Arguments	 : HeadIndex, &next-node-index
 * Description	 : This function physically deletes node that are logically deleted.
                    each logically deleted node is snipped using SNIP function
 * Remarks		 : Runs at work-item-level
 */
uint CLEAN(TLLNode *pNodePool, /* Hash-table + stack of free nodes */
		   uint HeadIndex, /* pointer to free-node in pNodePool */
		   uint *nextNodeIndex)
{
    uint uiLLNode = 0 ;
    uint status = 0, pBits = 0 ;
    uint uiLLMNextNode = 0 , uiLLNextNode = 0 ;

    uiLLNode = HeadIndex ;
    *nextNodeIndex = 0 ;

    while(true) {
        uiLLMNextNode = pNodePool[uiLLNode].uiNext ;
        uiLLNextNode  = GET_PTR(uiLLMNextNode) ;
        pBits = GET_BITS(uiLLMNextNode) ;
        status = SNIP(pNodePool, uiLLNode) ; // do it one-by-one
        if(status == HTS_NEXT_UNMARKED) {
            *nextNodeIndex = uiLLNextNode ; /* move to next node */
            return status ;
        } else if(status == HTS_INVALID_START) {
            if(( !IS_SET(pBits,2)) & (IS_SET(pBits,1))) {
                uiLLNode = uiLLNextNode ;
            } else { 
                if(uiLLNode == HeadIndex) {
                    return HTS_INVALID_START ;
                } else uiLLNode = HeadIndex ;
            } 
        } // end-of-first-if		
    } // end-of-while
} // end-of-CLEAN


/************************* WINDOW *****************************/
 /* Function name : WINDOW
  * Arguments	  : key, prevNodeIndex, nextNodeIndex, &index
  * Description	  : This function returns index of node that a given key can be inserted
  * Remarks		  : work-group-level function
  */
uint WINDOW(TLLNode *pNodePool,	/* Hash-table + stack of free nodes */
			uint key,
			uint prevNodeIndex, 
            uint nextNodeIndex,
			uint *index)
{
    uint uiLLMNode = 0, uiLLNode = 0 ;
    uint uiLLMNextNode = 0, uiLLNextNode = 0 ;
    uint status = 0 ;
    uint pBits = 0, nBits = 0 ;
    uint pMaxVal = 0 ;

    uint lid = get_local_id(0) ;

    if(prevNodeIndex != 0) {
        uiLLMNode = pNodePool[prevNodeIndex].uiNext ;
        uiLLNode = GET_PTR(uiLLMNode) ;
        pBits = GET_BITS(uiLLMNode) ;
        if( IS_SET(pBits,2) || IS_SET(pBits,0) ) 
            return HTS_INVALID_PREV_BITS ;
        if( uiLLNode != nextNodeIndex) 
            return HTS_INVALID_NEXT_REF ;
        uint uiVal = pNodePool[prevNodeIndex].pE[lid];
        pMaxVal = work_group_reduce_max(uiVal) ;
        if(key <= pMaxVal) return HTS_WINDOW_NOT_FOUND ;							
    }

    uiLLMNextNode = pNodePool[nextNodeIndex].uiNext ;
    uiLLNextNode = GET_PTR(uiLLMNextNode) ;
    nBits = GET_BITS(uiLLMNextNode) ;
    if( IS_SET(nBits,0) || IS_SET(nBits,2) ) {
        return HTS_INVALID_NEXT_BITS ;
    }
    uint uiVal = pNodePool[nextNodeIndex].pE[lid];

    pMaxVal = work_group_reduce_max(uiVal) ;
    if(key <= pMaxVal) { // TODO : check from here
        if(key == uiVal) {
            *index = lid + 1 ;
            status = HTS_KEY_FOUND ;
        } else {
            *index = 0 ;
             status = HTS_WINDOW_FOUND ;
        }
        *index = work_group_reduce_max(*index);
        if(*index) { // broadcast key 
            (*index) = work_group_broadcast(*index, (*index)-1) ;
            status = work_group_broadcast(status,(*index)-1) ;
        }
        work_group_barrier(CLK_GLOBAL_MEM_FENCE|CLK_LOCAL_MEM_FENCE);
        return status ;
    } 
    else return HTS_WINDOW_NOT_FOUND ;
} // end-of WINDOW


/*
** uiHashFunction:
** hash function to map key to hash table index.
*/
uint uiHashFunction(uint uiKey) 
{
    return uiKey & OCL_HASH_TABLE_MASK ;
}

/************************* bFind *****************************/
/* Function name	: bFind
 * Inputs			: NodePool, key
 * Outputs			: previousNode, NextNode, Index of key(if found)
 * Description		: work-group level function. searches for Key in the set, 
 *					  if key found return index of it, otherwise prevNode and nextNode
					  where the key can be inserted.	
 */
uint bFind( TLLNode* pNodePool,		/* Hash-table + stack of free nodes */		
		   uint     Key,			/* Key to be find */
           uint*	prevNodeIndex,  /* if key not found, return window */
           uint*	nextNodeIndex,
           uint*    Index)			/* if key found, return its index */
{
    uint pRef = 0, nRef = 0 ;
    uint status = 0, status2 = 0 ;
    bool bNodeFound = false ;

    // get the thread id
    uint lid = get_local_id(0) ;

    // get the starting node
    uint uiPPtr      = uiHashFunction(Key) ;

    *prevNodeIndex = 0 ;
	*nextNodeIndex = 0 ;
    *Index = 0 ;

    pRef = 0 ;
    nRef = uiPPtr ;

	status = HTS_WINDOW_NOT_FOUND ;

    while(status == HTS_WINDOW_NOT_FOUND) {

		// get the window of key to be inserted
        status = WINDOW(pNodePool, Key, pRef, nRef, Index) ;

		// window not found !!!
        if(status == HTS_WINDOW_NOT_FOUND) {
            pRef = nRef ;
            if(lid == 0) {
                status2 = CLEAN(pNodePool, pRef, &nRef) ;
                if(status2==HTS_INVALID_START)
                    status = HTS_WINDOW_NOT_FOUND;
            } 
            work_group_barrier(CLK_GLOBAL_MEM_FENCE);
            status = work_group_broadcast(status,0) ;
            nRef = work_group_broadcast(nRef,0) ; 
        }
		// handling boundary conditions --> TODO : check
        if( nRef == 0) {
			status = HTS_WINDOW_FOUND ; 
		}
    } // end-of-while*/

    if(status == HTS_WINDOW_FOUND) { /* key not found, but window found */
        *prevNodeIndex = pRef ;
        *nextNodeIndex = nRef ;
        return status ;
    }
    if(status == HTS_KEY_FOUND) { /* key found */
        return status ;
    }

    return status ;
} // end of bFind()



// kernel calling from host
__kernel void HTSTopKernel(__global void* pvOclReqQueue,
                           __global void* pvNodePool,
                           __global void* pvMiscData,
                           uint  uiReqCount)
    {
    uint uiFlags = 0 ;
    uint uiKey = 0 ;
    uint uiStatus = 0 ;
    uint uiType = 0 ;
    uint uiretVal = 0 ; 
    bool bReqStatus = false ;
    
    //get the svm data structures
    TQueuedRequest* pOclReqQueue = (TQueuedRequest *)pvOclReqQueue ;
    TLLNode*        pNodePool    = (TLLNode *)pvNodePool ; // hash table + free node pool
    TMiscData*      pMiscData    = (TMiscData*)pvMiscData ;
    uint            uiHeadIndex =  pMiscData->uiHeadIndex; // points to head of free nodes pool

    uint grid = get_group_id(0);
    uint lid  = get_local_id(0);

    if (grid < uiReqCount)
        {
        uiKey   = pOclReqQueue[grid].uiKey; 
        uiType  = pOclReqQueue[grid].uiType;
        uiFlags = pOclReqQueue[grid].uiFlags;

        uint uiNode = 0, uiIndex = 0 ;
        uint prevNodeIndex = 0, nextNodeIndex = 0 ;

        if(uiType == HTS_REQ_TYPE_FIND) 
            {
				/* when key_found, no need to worry about prevNodeIndex and nextNodeIndex */
				uiretVal = bFind(pNodePool,
							uiKey,
							&prevNodeIndex,
							&nextNodeIndex,
							&uiIndex);
                if(uiIndex) bReqStatus = true ;
            }
        else if(uiType == HTS_REQ_TYPE_ADD)
            {
                bReqStatus = false ;
            }
        else if(uiType == HTS_REQ_TYPE_REMOVE)
            {
                bReqStatus = false ;
            }

        work_group_barrier(CLK_GLOBAL_MEM_FENCE|CLK_LOCAL_MEM_FENCE);

        if(bReqStatus == true)
            uiIndex = 1;
        else
            uiIndex = 0;

        if(lid == 0)
            {
            uiFlags = SET_FLAG(uiFlags,HTS_REQ_COMPLETED);
            pOclReqQueue[grid].uiFlags  = uiFlags;
            pOclReqQueue[grid].uiStatus = uiIndex;
            }
        work_group_barrier(CLK_GLOBAL_MEM_FENCE|CLK_LOCAL_MEM_FENCE);

        } 
    }



