#![allow(non_snake_case)]

extern crate timely;
use timely::dataflow::operators::Inspect;

extern crate rdkafka;
use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, BaseConsumer, EmptyConsumerContext};

fn main() {

    let mut args = ::std::env::args();
    args.next();

    // Extract Kafka topic.
    let topic = args.next().expect("Must specify a Kafka topic");
    let brokers = "localhost:9092";

    // Create Kafka consumer configuration.
    // Feel free to change parameters here.
    let mut consumer_config = ClientConfig::new();
    consumer_config
        .set("produce.offset.report", "true")
        .set("auto.offset.reset", "smallest")
        .set("group.id", "example")
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000")
        .set("bootstrap.servers", &brokers);

    timely::execute_from_args(args, move |worker| {

        // A dataflow for producing spans.
        worker.dataflow::<u64,_,_>(|scope| {

            // Create a Kafka consumer.
            let consumer : BaseConsumer<EmptyConsumerContext> = consumer_config.create().expect("Couldn't create consumer");
            consumer.subscribe(&[&topic]).expect("Failed to subscribe to topic");


            // A stream of strongly typed `Span` records.
            let spans =
            kafka_source(scope, "KafkaSource", consumer, |payload, output, capability| {

                if let Ok(text) = std::str::from_utf8(payload) {
                    output.session(&capability)
                          .give(text.to_string());
                }
                else {
                    println!("Failed to decode payload as utf8.");
                }

            });

            spans.inspect(|x| println!("Observed: {:?}", x));

        });

    }).expect("Timely computation failed somehow");

    println!("Hello, world!");
}

use timely::Data;
use timely::dataflow::{Scope, Stream};
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::Capability;
use timely::dataflow::operators::generic::OutputHandle;

pub fn kafka_source<G, D, L>(
    scope: &mut G,
    name: &str,
    consumer: BaseConsumer<EmptyConsumerContext>,
    mut logic: L) -> Stream<G, D>
where
    G: Scope,
    D: Data,
    L: FnMut(&[u8], &mut OutputHandle<G::Timestamp, D, Tee<G::Timestamp, D>>, &mut Capability<G::Timestamp>) + 'static,
{

    timely::dataflow::operators::generic::source(scope, name, |capability, info| {

        let activator = scope.activator_for(&info.address[..]);
        let mut cap = Some(capability);

        // define a closure to call repeatedly.
        move |output| {

            // Act only if we retain the capability to send data.
            if let Some(capability) = cap.as_mut() {

                // Indicate that we should run again.
                activator.activate();

                // Repeatedly interrogate Kafka for [u8] messages.
                // Cease only when Kafka stops returning new data.
                // Could cease earlier, if we had a better policy.
                while let Some(result) = consumer.poll(0) {

                    // If valid data back from Kafka
                    if let Ok(message) = result {
                        // Attempt to interpret bytes as utf8  ...
                        if let Some(payload) = message.payload() {

                            // Do something with the bytes ..
                            logic(payload, output, capability);
                        }
                        else {
                            println!("No payload received");
                        }
                    }
                    else {
                        println!("Kafka error");
                    }
                }
            }

        }
    })

}