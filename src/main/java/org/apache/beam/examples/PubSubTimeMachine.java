// Copyright 2019 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.apache.beam.examples;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.Instant;

public class PubSubTimeMachine {

  public interface PubSubToGcsOptions extends PipelineOptions, StreamingOptions {
    @Description("The Cloud Pub/Sub topic to read from.")
    @Required
    String getInputTopic();
  }

  static class PubsubMessageToStateObject extends DoFn<PubsubMessage, KV<String,StateMachineObject>> {
    @ProcessElement
    public void processElement(@Element PubsubMessage pubsubMessage, OutputReceiver<KV<String,StateMachineObject>> out) throws IOException {
      // Use OutputReceiver.output to emit the output element.
      String json = new String(pubsubMessage.getPayload(), StandardCharsets.UTF_8);
      ObjectMapper objectMapper = new ObjectMapper();
      StateMachineObject stateMachineObject = objectMapper.readValue(json, StateMachineObject.class);
      out.output(KV.of(String.valueOf(stateMachineObject.getKey()), stateMachineObject));
    }
  }

  static class StateMachineDoFn extends DoFn<KV<String,StateMachineObject>, KV<String,StateMachineObject>> {
    @StateId("state") private final StateSpec<BagState<StateMachineObject>> stateObject = StateSpecs.bag();
    @StateId("minTimestampInBag") private final StateSpec<CombiningState<Long, long[], Long>>
            minTimestampInBag = StateSpecs.combining(Min.ofLongs());
    @TimerFamily("timer") private final TimerSpec timer = TimerSpecs.timerMap(TimeDomain.PROCESSING_TIME);

    @ProcessElement
    public void processElement(@Element KV<String,StateMachineObject> object,
                               @StateId("state") BagState<StateMachineObject> state,
                               @TimerFamily("timer") TimerMap timer,
                               @AlwaysFetched @StateId("minTimestampInBag") CombiningState<Long, long[], Long> minTimestamp,
                               OutputReceiver<KV<String,StateMachineObject>> out) throws IOException {
      // Use OutputReceiver.output to emit the output element.
      StateMachineObject stateMachineObject = object.getValue();
      state.add(stateMachineObject);
      Instant elementTs = Instant.ofEpochMilli(stateMachineObject.getTimestamp()
              + stateMachineObject.getUrgencyInMillis());

      minTimestamp.add(elementTs.getMillis());

      System.out.println("Set Timer for key: " + stateMachineObject.getKey()+
              " ts: " +Instant.ofEpochMilli(minTimestamp.read()));

      timer.set(String.valueOf(stateMachineObject.getKey()),
              Instant.ofEpochMilli(minTimestamp.read()));
    }

    @OnTimerFamily("timer") public void onTimer( @StateId("state") BagState<StateMachineObject> state,
                                                 @TimerId String timerId,@TimerFamily("timer") TimerMap timer) {
      System.out.println("Ontimer for id: " + timerId + " ts: " +Instant.now().getMillis()+
              " humanreadable TS: " + Instant.now());

      Iterable<StateMachineObject> stateMachineObjects = state.read();
      state.clear();
      for (StateMachineObject s: stateMachineObjects) {
        if(Integer.parseInt(timerId) == s.getKey() &&
                Instant.now().getMillis() >= s.getTriggerInMillis()){
          System.out.println("Trigger for key: " + s.getKey()+" and state: "+ s.getState() +
                  " ts: " +s.getTriggerInMillis() + " humanreadable TS: " + Instant.ofEpochMilli(s.getTriggerInMillis()));
          continue;
        }
        state.add(s);
      }
      stateMachineObjects = state.read();
      for (StateMachineObject s: stateMachineObjects) {
        if(Integer.parseInt(timerId) == s.getKey()){
          timer.set(timerId, Instant.ofEpochMilli(s.getTimestamp()+s.getUrgencyInMillis()));
        }
      }


      //Process timer.
    }
  }


  public static void main(String[] args) throws IOException {
    // The maximum number of shards when writing output.
    int numShards = 1;

    PubSubToGcsOptions options =
            PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubToGcsOptions.class);

    options.setStreaming(true);

    Pipeline pipeline = Pipeline.create(options);

    pipeline
            // 1) Read string messages from a Pub/Sub topic.
            .apply("Read PubSub Messages", PubsubIO.readMessages().fromTopic(options.getInputTopic()))
            .apply(ParDo.of(new PubsubMessageToStateObject()))
            .apply(ParDo.of(new StateMachineDoFn()));
            // 3) Write one file to GCS for every window of messages.
//            .apply("Write Files to GCS", new WriteOneFilePerWindow(options.getOutput(), numShards));

    // Execute the pipeline and wait until it finishes running.
    pipeline.run().waitUntilFinish();
  }
}
// [END pubsub_to_gcs]