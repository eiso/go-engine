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

### Setting up your k8s cluster

```
# gcloud container clusters delete gleam
gcloud container clusters create gleam
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

gcloud compute instances detach-disk gke-gleam-default-pool-d58b1f3f-zgw3 --disk gleam-pv-disk