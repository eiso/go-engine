## Guide for distributed gleam on Google Cloud Kubernetes Engine

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

### Build the necessary binaries
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

```
cd _examples/driver/
```

Now apply the k8s configuration files already provided:
```
kubectl apply -f k8s/
```

In case you see an error: `namespaces "gleam" not found`. Run the above command again, sometimes GCP takes a bit of time to propogate the namespaces.

Inspect the pods created if you encounter any errors:
```
kubectl config view | grep namespace:
kubectl get pods
kubectl get events
kubectl describe pod master
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
kubectl logs driver-gq4k8
```

#### Launch the gleam web-ui 
```
kubectl expose deployment master --type=LoadBalancer --name=web-ui
kubectl get services web-ui
kubectl describe services web-ui
```

It can take up to several mintues for an external IP to be assigned to your load balancer.

```
Go to: http://EXTERNAL-IP:45326
```

### SSH into a running pod

```
kubectl exec -it agent-786073843-zxjxj -- /bin/sh
```
