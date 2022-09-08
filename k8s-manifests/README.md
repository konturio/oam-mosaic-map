# Create k8s resources

## Create and verify secrets
```bash
$ cat << EOT >> secret.yaml
apiVersion: v1
kind: Secret
metadata:
  name: raster-tiler
  namespace: dev-mosaic
  labels:
    app.kubernetes.io/managed-by: manually
    app.kubernetes.io/name: raster-tiler
    app.kubernetes.io/instance: raster-tiler-abcxzy
    environment: dev
data:
  username: c29tZXVzZXJuYW1l
  password: c29tZXBhc3N3b3Jk
EOT
> Secret values should be base64 encoded

$ kubectl apply -f secret.yaml 

$ kubectl -n dev-mosaic get secret
NAME                  TYPE                                  DATA   AGE
default-token-jv2cd   kubernetes.io/service-account-token   3      41s
raster-tiler          Opaque                                2      26s
```

## Create and verify PVC
```bash
$ kubectl apply -f pvc.yaml
persistentvolumeclaim/raster-tiler created

$ kubectl -n dev-mosaic get pvc
NAME           STATUS    VOLUME   CAPACITY   ACCESS MODES   STORAGECLASS   AGE
raster-tiler   Pending                                      local-path     17s
```
Will be in pending state untill attach to pod

## Create and verify deployment
```bash
$ kubectl -n dev-mosaic get deploy
NAME           READY   UP-TO-DATE   AVAILABLE   AGE
raster-tiler   1/1     1            1           24s
$ kubectl -n dev-mosaic get po
NAME                           READY   STATUS    RESTARTS   AGE
raster-tiler-5bfc48bc6-9hcjz   1/1     Running   0          29s
```

Once pod created `pvc` should be in `Bound` status
```bash
kubectl -n dev-mosaic get pvc
NAME           STATUS   VOLUME                                     CAPACITY   ACCESS MODES   STORAGECLASS   AGE
raster-tiler   Bound    pvc-ba748bb7-6cff-45f0-8f79-0ff97bef31f2   10Gi       RWO            local-path     13m
```
You also can `exec` into pod and verify volume
```bash
$ kubectl -n dev-mosaic exec -it raster-tiler-64cdbcf5fd-hjqtv -- bash
root@raster-tiler-64cdbcf5fd-hjqtv:/# ls -l /tmp
total 0
drwxrwxrwx. 2 root root 6 Sep  8 04:09 raster-tiler
root@raster-tiler-64cdbcf5fd-hjqtv:/# ls -l /tmp/raster-tiler/
total 0
root@raster-tiler-64cdbcf5fd-hjqtv:/#
```

## Create and verify service
```bash
$ kubectl apply -f service.yaml
service/raster-tiler created
kubectl -n dev-mosaic get svc
NAME           TYPE        CLUSTER-IP       EXTERNAL-IP   PORT(S)   AGE
raster-tiler   ClusterIP   10.102.172.206   <none>        80/TCP    21s
```

Now you already can access this service in browser without ingress. But better use ingress
```bash
$ kubectl port-forward -n dev-mosaic  svc/raster-tiler 8888:80
Forwarding from 127.0.0.1:8888 -> 80
Forwarding from [::1]:8888 -> 80
Handling connection for 8888
Handling connection for 8888

$ curl -IXGET localhost:8888
HTTP/1.1 200 OK
Server: nginx/1.19.10
Date: Thu, 08 Sep 2022 04:18:57 GMT
Content-Type: text/html
Content-Length: 612
Last-Modified: Tue, 13 Apr 2021 15:13:59 GMT
Connection: keep-alive
ETag: "6075b537-264"
Accept-Ranges: bytes
```

## Create and verify ingress
```bash
$ kubectl apply -f ingress.yaml
ingress.networking.k8s.io/raster-tiler created

$ kubectl -n dev-mosaic get ing
NAME           CLASS   HOSTS                                ADDRESS       PORTS     AGE
raster-tiler   nginx   raster-tiler.k8s-01.konturlabs.com   46.4.70.177   80, 443   19s

$ curl -IXGET raster-tiler.k8s-01.konturlabs.com
HTTP/1.1 301 Moved Permanently
Server: nginx/1.21.6
Date: Thu, 08 Sep 2022 04:23:02 GMT
Content-Type: text/html
Content-Length: 169
Connection: keep-alive
Location: https://raster-tiler.k8s-01.konturlabs.com:443/
```

## Delete k8s resources
```bash
$ kubectl delete -f ingress.yaml
ingress.networking.k8s.io "raster-tiler" deleted
$ kubectl delete -f service.yaml
service "raster-tiler" deleted
$ kubectl delete -f deployment.yaml
deployment.apps "raster-tiler" deleted
$ kubectl delete -f pvc.yaml
persistentvolumeclaim "raster-tiler" deleted
$ kubectl delete -f secret.yaml
secret "raster-tiler" deleted
```