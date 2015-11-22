.PHONY: help
help:
	$(info )
	$(info This makefile will help you build docker images for Bitergia Gelk platform )
	$(info )
	$(info Available options:)
	$(info )
	$(info - all:			Build all available images)
	$(info - gelk:			Build gelk image)
	$(info )
	@echo ""

.PHONY: gelk
gelk: $@
	docker build -t bitergia/$@ $@/

.PHONY: all
all: gelk

.PHONY: clean clean_gelk
clean: clean_gelk

clean_gelk:
	-@docker rmi bitergia/gelk
