.PHONY: release

DOCKER_IMAGE = "docker.io/dvincelli/logrepl"

release:
	echo "Building release version"
	@echo "Enter the version number: "
	@read VERSION; \
	git tag -l | grep -q $(VERSION) && echo "Version $(VERSION) already exists" && exit 1
	docker build -t $(DOCKER_IMAGE):$(VERSION) .
	git tag -a $(VERSION) -m "Release $(VERSION)"
	docker push $(DOCKER_IMAGE):$(VERSION)
