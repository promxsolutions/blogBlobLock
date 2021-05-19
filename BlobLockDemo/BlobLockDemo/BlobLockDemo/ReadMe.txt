
StartDemoFunction will get triggered from outside.
CoreDemoFunction will be doing heavy lifting and requires protection via Lock.

Both functions are 'connected' via Queue:
- StartDemoFunction will put message into the queue;
- CoreDemoFunction will be triggered by the new message in the queue.