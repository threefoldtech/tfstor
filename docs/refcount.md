# Reference Counting (Refcount)

Refcount adds reference counting to data blocks.

The idea is that different objects might share the same data blocks. 
When a data block is used by an object, its refcount is increased by one. 
When an object stops using the data block, the refcount is decreased by one. 
When the refcount reaches zero, the data block can be deleted.

`There can be some data leakage, but never data loss`. This means that, in case of some failures, it is acceptable to not decrease the refcount. However, it is crucial to never fail to increase the refcount, as this can lead to data loss.

## Test Plan

**Normal cases:**

- Creating a new object will set `refcount` to 1.
- If the same key reuses the block, `refcount` will not be increased.
- If a new key reuses the block, `refcount` will be increased by one.
- Deleting the object will reduce the `refcount` value.
- When the `refcount` value reaches zero, the object must be deleted.
- if a key is updated with different blocks, if the block is not used anymore its `refcount` also needs to be decreased.

**Failure cases:**
- when writing/reusing block, failed to write the metadata or increase the `refcount`
      
    -> it is not allowed to happen, the writing/reusing must be failed, data loss could happend

- when writing the block, `refcount` already increased but failed to write the block to the underlying storage.
    
    -> it is OK, data leakage could happen

- when deleting a block, failed to reduce the `refcount` or delete the metadata
    
    -> it is OK, data leakage could happen

- when deleting a block, failed to delete from the underlying storage
    
    -> it is OK, data leakage happened

For the above failure cases, the tests only mandatory to be implemented for the cases that not allowed to happen