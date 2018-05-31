prefix = $(shell whoami)_
dataset = $(prefix)temp
project =
topic =
schemas = $(wildcard ./src/main/resources/schemas/*.json)
schema_files = $(basename $(notdir $(schemas)))
vpath %.json ./src/main/resources/schemas

.PHONY: run_side_input
run_side_input:
	sbt "runMain com.mrcalonso.RefreshingSideInput \
		--project=$(project) --runner=DataflowRunner --region=europe-west1 --zone=europe-west1-b \
		--autoscalingAlgorithm=THROUGHPUT_BASED \
		--maxNumWorkers=5 --jobName=$(subst _,-,$(prefix))rsi-test \
		--workerLogLevelOverrides={\"com.mrcalonso\":\"DEBUG\"} \
		--subscription=$(prefix)rsi-test --dataset=$(dataset)"

.PHONY: run_pcollection
run_pcollection:
	sbt "runMain com.mrcalonso.RefreshingSideInput2 \
		--project=$(project) --runner=DataflowRunner --region=europe-west1 --zone=europe-west1-b \
		--autoscalingAlgorithm=THROUGHPUT_BASED \
		--maxNumWorkers=5 --jobName=$(subst _,-,$(prefix))rsi-test \
		--workerLogLevelOverrides={\"com.mrcalonso\":\"DEBUG\"} \
		--subscription=$(prefix)rsi-test --dataset=$(dataset) --refreshFreq=2"

.PHONY: create_test_subscription
create_test_subscription:
	gcloud beta pubsub subscriptions create $(prefix)rsi-test --topic projects/$(project)/topics/$(topic)

.PHONY: delete_test_subscription
delete_test_subscription:
	gcloud beta pubsub subscriptions delete $(prefix)rsi-test

patch_%: %.json
	set -e ;\
	token="$$(gcloud auth application-default print-access-token)" ;\
	data=`cat $<` ;\
	data2="{\"schema\":{\"fields\":$$data}}" ;\
	curl 'https://www.googleapis.com/bigquery/v2/projects/$(project)/datasets/$(dataset)/tables/$(*F)' \
		-X PATCH \
		-H "Authorization: Bearer $$token" \
		-H 'Accept: application/json' \
		-H 'Content-Type: application/json' \
		--data-binary "$$data2" \
		--compressed

create_%: %.json
	bq mk $(dataset).$(*F) $<

.PHONY: drop_tables
drop_tables:
	for f in $(schema_files); do\
		name=$$( echo $$f | cut -d . -f1 );\
		bq rm -f $(dataset).$$name;\
	done