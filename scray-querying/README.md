Synchronization-API

# Sync-API
Api to synchronize online and batch processes.
Online and batch processes are synchronized by an attribut which exists in both processes e.g. time.

## Assumptions
- Batch jobs work with a finite number of elements
- Elements in streaming jobs are ordered by sync attribut

## Some definitions...

- Timed Data: A dataset which can be associated with a point in time.

- Batch-Job: A "Batch-Job" is a function on timed data. Starting time and end time are parameters which restrict the domain of the function. Only timed data which associates a point in time inbetween starting time and end time is being processed.

- Batch-View: Precomputed data, called "Batch-View", is the result of a Batch-Job (c.f. [1]). Therefore, it has a defined starting and end-point in time for timed data which has been processed by the Batch-Job to produce this Batch-View. 

- Batch-Layer: A Batch-Layer is a facility to process timed data by executing Batch-Jobs and to store the results of the Batch-Jobs as Batch-Views.

- Eventually ordered sequence: A sequence with an ordering that can be established at an undefined but fixed point in time.

Stream: A stream is an eventually ordered sequence of timed data, ordered by the time associated with the timed data.

Online-Job: A function on a stream. There are Online-Jobs which merge existing Batch-Views with the results of a function on a stream. Starting time is a parameter which restricts the domain of the function. Only timed data which associates a point in time after starting time is being processed.

Online-View: Precomputed data, which is the result of an Online-Job. Therefore, it has a defined starting point in time when the online-job started processing the stream.

Online-Layer: An Online-Layer is a facility to process timed data by executing Online-Jobs and to store the results of the Online-Jobs as Online-Views.

Job: A job is a combination of a Batch-Job and an Online-Job. It has an identifier called "Job-Name".

Version: A version identifies starting and end time of a batch-view.

BatchID: A batchID is a representation of a version.

Slot: Given is a Batch- or Online-Layer which can only store a fixed number of Batch- or Online-Views. In such a system a place to store Batch- or Online-Views is called a slot.

Rolling version: Given is a Batch- or Online-Layer with a fixed number of slots for storing Batch- or Online-Views. A rolling version is the number of the slot and the associated data.

[1] Nathan Marz, James Warren: BigData, Manning, 2015.
