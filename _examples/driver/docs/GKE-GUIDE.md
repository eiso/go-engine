## Guide for running a distributed go-engine analyzing the Public Git Archive dataset using the Google Cloud Kubernetes Engine

### Local instructions for setting up kubectl 

```
sudo gcloud components install kubectl
sudo rm /usr/bin/kubectl 
```
fish: 
```
set -gx PATH $PATH /opt/google-cloud-sdk/bin/
``` 

bash: 
```
export PATH=$PATH:/opt/google-cloud-sdk
```

### Build the necessary binaries & docker containers
Be sure to update the registry address in the dockerfiles.
 
In case you're only changing the driver code (`_examples/driver/driver.go`) you only need to update `Dockerfile.driver`

```
cd go-engine/_examples/driver/docker/
vim Dockerfile.gleam
vim Dockerfile.driver
``` 

```
cd ../
make clean
make docker
```

### Defining your Google Cloud Node Pool

go-engine is CPU intensive but low memory consumption, therefore it is ~20% more affordable to use Google Cloud's [high-CPU instances](https://cloud.google.com/compute/docs/machine-types#highcpu).

```
gcloud container node-pools create go-engine-node-pool --cluster=gleam --disk-size=50 --image-type=cos --machine-type=n1-highcpu-4 --num-nodes=5
```

If you want to change an already running cluster but don't mind deleting your running deployment:

```
kubectl delete deployment master
kubectl delete deployment agent
# be sure to delete any remaining jobs
# kubectl delete job NAME
gcloud container node-pools delete go-engine-node-pool --cluster=gleam
# RUN ABOVE 'gcloud container node-pools create...' COMMAND
```

### Setting up your k8s cluster

```
# gcloud container clusters delete gleam
gcloud container clusters create gleam --num-nodes=5
gcloud container clusters get-credentials gleam
gcloud container clusters list
```

bash:
```
kubectl config set-context $(kubectl config current-context) --namespace=gleam
```
fish:
```
kubectl config set-context (kubectl config current-context) --namespace=gleam
```

Apply the gleam namespace

```
kubectl apply -f k8s/gleam-namespace.yaml
```

```
kubectl config view | grep namespace:

```

In case you see an error: `namespaces "gleam" not found`. Run the above command again, sometimes GCP takes a bit of time to propogate the namespaces.

#### Resizing your cluster

In case you need to resize you cluster:

```
gcloud container clusters resize gleam --node-pool go-engine-node-pool --size 6
```

### Load the Public Git Archive dataset with pga

Creates a 3TB persistent volume and a persistent volume claim that can only be written too by one pod/job:

```
gcloud compute disks create gleam-pv-disk --size 3000 --type pd-standard
gcloud compute disks describe gleam-pv-disk
```

```
kubectl apply -f k8s/dataset/
```

Creates the job that will download the data (~8h30 hours to download the dataset):

```
kubectl create -f k8s/jobs/pga-job.yaml 
# kubectl delete job pga
```

Inspect if it is running successfully: 

```
kubectl describe job pga
kubectl get pods
kubectl describe pods pga-jw4mx
kubectl logs pga-jw4mx
```

To see how much data has already been downloaded:

```
kubectl exec -it pga-52bl8 -- du -h /data | tail -1
```

To count the # of repositories while downloading:

```
kubectl exec -it pga-52bl8 -- find /data -type f | wc -l
```

Removes the persistent volume claim from the disk

```
kubectl delete pvc gleam-pvc
```

Patches the persistent volume to be read only by multiple pods and removes the binding to the specif uid to free it up:

```
kubectl patch pv gleam-pv --patch "spec:
    accessModes:
      - ReadOnlyMany
    claimRef:
      uid:
      resourceVersion:"
```

```
gcloud compute instances detach-disk gke-gleam-default-pool-d58b1f3f-zgw3 --disk gleam-pv-disk
```

#### Optional

Now create a snapshot of the disk:

```
gcloud compute disks snapshot gleam-pv-disk
```

Create a second disk from this snapshot:

```
gcloud compute disks create gleam-pv-disk-clone \
  --source-snapshot=h5nendogj502
```

### Setting up gleam

```
gcloud compute instances detach-disk gke-gleam-default-pool-d58b1f3f-zgw3 --disk gleam-pv-disk
```

Now apply the k8s configuration files already provided:

```
kubectl apply -f k8s/gleam/
```

Inspect the pods created if you encounter any errors:

```
kubectl get pods
kubectl get events
kubectl describe pod master
```

```
# kubectl get deployments
# kubectl delete deployment master
# kubectl delete deployment agent
# kubectl get services
# kubectl delete service master
# kubectl get pvc
# kubectl delete pvc gleam-pvc
# kubectl get pv
# kubectl delete pv gleam-pv
```

#### Launch the gleam web-ui 
```
kubectl expose deployment master --type=LoadBalancer --name=web-ui
kubectl describe services web-ui
kubectl get services web-ui
```

If you see `EXTERNAL IP <pending>`, know that it can take up to several mintues for an external IP to be assigned to your load balancer.

```
Go to: http://EXTERNAL-IP:45326
```

### Execute the driver as a job

In k8s jobs run till completion. An example of a job running a query on the driver is in `k8s/jobs/driver-job.yaml`

```
kubectl create -f k8s/jobs/driver-job.yaml
# kubectl delete job driver
```

See if the driver ran successfully:
```
kubectl get jobs
kubectl describe job driver
kubectl describe pods driver-gq4k8
kubectl logs driver-gq4k8
kubectl get pods
```

### Other

#### SSH into a running pod

```
kubectl exec -it agent-786073843-zxjxj -- /bin/sh
```

#### Detaching a disk

```
gcloud compute instances detach-disk gke-gleam-default-pool-d58b1f3f-zgw3 --disk gleam-pv-disk
```

#### Debugging empty downloads

Find all files of size 0

```
find /data/siva/latest/ -type f -size 0c -exec ls {} \;
```

Find all files of size >0

```
find /data/siva/latest/ea/ -type f -size +0c -exec ls {} \;
```

#### See logs of previous pod after a restart

#### Expose 8080 to run pprof on the driver

```
kubectl expose deployment agent --type=LoadBalancer --name=pprof
kubectl describe services pprof
kubectl get services pprof
```

To see memory usage:

```
go tool pprof http://EXTERNAL-IP:8080/debug/pprof/heap
top
```