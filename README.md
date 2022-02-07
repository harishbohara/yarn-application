This is a complete end-to-end Yarn application. It will do the following:
1. Submit yarn job to yarn cluster
2. Start an application manager
3. Application manager will launch 3 more container to run this app

## How to run
1. Run ```YarnSimpleLsCommandRunOnYarn```, which will send yoy application manger to run on Yarn
  
Note - I have added the application manager and the application all in this single project. Change 
```YarnSimpleLsCommandRunOnYarn``` which has hard coded path to my jar in main method 
```bigdata-1.0-SNAPSHOT-jar-with-dependencies.jar```

2. Once you run it, you will see a job is summited to YARN. This is the application manager

3. Once application manager starts, it will ask for 3 more container to run which will run the app 
```com.harish.yarn.application.Application```. This is a dummy app which just prints number in the loop.
