# Cloud-Task-Execution-Framework
Implementation of Task Execution Framework using Amazon's cloud services like SQS and DynamoDB

This document presents with the performance evaluation for:<br/>
<ol>
  <li><strong>Local Back-end Worker</strong></li>
  <li><strong>Remote Back-end Worker</strong></li>
  <li><strong>Animoto Video</strong></li>
</ol>
The diagram below shows the overview of Task Execution Framework for my implementation.<br/> 
It consists of a Client, 2 Queues (SQS/ in-memory), Workers (Remote/ Local) and DynamoDB (Remote Workers).<br/>
<strong>The Client</strong> is presented with a workload file which has tasks (sleep tasks, Image URLs file location). The client then puts the jobs onto the SQS tasks queue.<br/> 
<strong>The Worker</strong> executes the jobs and writes the response back to response queue. DynamoDB interaction is with worker to check for already executed tasks. <br/>
There is a <strong>S3</strong> component involved for storing the videos in case of Animoto Clone for converting images to video.<br/>
The instance used for evaluation purpose is t2.micro, linux version: ubutnu and region is us-east-1 (N. Virginia)

<img src="architecture.jpg" alt=""></img>
