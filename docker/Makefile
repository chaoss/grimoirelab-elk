.PHONY: help
help:
	$(info )
	$(info This makefile will help you build docker images for Bitergia Gelk platform )
	$(info )
	$(info Available options:)
	$(info )
	$(info - all:			Build all available images)
	$(info - gelk:			Build gelk image)
	$(info - gelk-arthur:	Build gelk-arthur image)
	$(info - gelk-lite:	    Minimal gelk version)
	$(info )
	@echo ""

.PHONY: gelk-lite
gelk-lite: $@
	docker build -t bitergia/$@ $@/

.PHONY: gelk
gelk: $@
	docker build -t bitergia/$@ $@/

.PHONY: gelk-arthur
gelk-arthur: $@
	docker build -t bitergia/$@ $@/

.PHONY: all
all: gelk gelk-arthur

.PHONY: clean clean_gelk clean_gelk-arthur clean_gelk-lite
clean: clean_gelk clean_gelk-arthur clean_gelk-lite

clean_gelk-lite:
	-@docker rmi bitergia/gelk-lite

clean_gelk:
	-@docker rmi bitergia/gelk

clean_gelk-arthur:
	-@docker rmi bitergia/gelk-arthur
