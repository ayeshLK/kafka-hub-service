# Publishing Docker Images # 

Here we will be considering to publish the docker image for `WSO2 Identity Server`. 

## Building & Publishing ##

* Dokcer uses tags to control the version of the images.
* Usually when publishing tags there will be two versions. Current version and the `latest` tags.
* Run following to publish the current tag.
```
docker build --file=./Dockerfile.wso2.is -t ayeshalmeida/wso2-is:5.11.0 --rm=true .
docker push ayeshalmeida/wso2-is:5.11.0
```

* Run following to publish latest tag.
```
docker build --file=./Dockerfile.wso2.is -t ayeshalmeida/wso2-is:latest --rm=true .
docker push ayeshalmeida/wso2-is:latest
```


