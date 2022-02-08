package io.confluent.developer.spring;

import com.github.javafaker.Faker;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Arrays;
import java.util.stream.Stream;

@SpringBootApplication
@EnableKafkaStreams
public class  SpringCcloudApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringCcloudApplication.class, args);
	}

	@Bean
	NewTopic hobbit2() //Criar novo topico por codigo
	{

		return TopicBuilder.name("hobbit2").partitions(12).replicas(3).build(); //criar um novo topico dentro do cluster
	}

	@Bean
	NewTopic counts() // Criar topico para o consumo da contagem
	{
		return TopicBuilder.name("streams-wordcount-output").partitions(6).replicas(3).build();
	}


	@RequiredArgsConstructor
	@Component
	class Producer
	{

		private final KafkaTemplate<Integer, String> template;

		Faker faker; //instanciando a dependencia da Classe

		@EventListener(ApplicationStartedEvent.class)
		public void generate()
		{
			faker = Faker.instance();
			final Flux<Long> interval = Flux.interval(Duration.ofMillis(1_000)); // gerar mensagem a cada segundo

			final Flux<String> quotes = Flux.fromStream(Stream.generate(() -> faker.hobbit().quote())); // gerar a String da Classe definida

			// enviar para o topico a string, definindo o topico
			Flux.zip(interval, quotes).map(it -> template.send("hobbit", faker.random().nextInt(42), it.getT2())).blockLast();
		}
	}

	@Component
	class Consumer
	{

		@KafkaListener(topics = {"streams-wordcount-output"}, groupId = "spring-boot-kafka")
		// definir o topico a ser consumido e o nome do consumer no groupId que sera exibido na Confluent exemplo: "ConsumidorHobbit-1"
		public void consume(ConsumerRecord<String, Long> record)
		{
			System.out.println("received = " + record.value() + " with key " + record.key()); //printar o que foi consumido do topico
		}
	}

	@Component
	class Processor {

		//adicao do construtor de streams, isso habilitando com a anotacao @EnableKafkaStreams na aplication
		@Autowired
		public void process(StreamsBuilder builder) {

			//inicializar as Serdes de String e Integer
			final Serde<Integer> integerSerde = Serdes.Integer();
			final Serde<String> stringSerde = Serdes.String();
			final Serde<Long> longSerde = Serdes.Long();

			//Alterar tipo conforme o Topico
			KStream<Integer, String> textLines = builder.stream("hobbit", Consumed.with(integerSerde, stringSerde));

			KTable<String, Long> wordCounts = textLines
					.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
					.groupBy((key, value) -> value, Grouped.with(stringSerde, stringSerde))
					.count(Materialized.as("counts"));

		wordCounts.toStream().to("streams-wordcount-output", Produced.with(stringSerde, longSerde));

		}
	}


	@RestController
	@RequiredArgsConstructor
	class RestService{

		//Configurar um endpoint para verificar a quantidade de palavras consumidas no topico

		private final StreamsBuilderFactoryBean factoryBean;

		@GetMapping("/count/{word}")
		public Long getCount(@PathVariable String word){
			final KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();

			ReadOnlyKeyValueStore<String, Long> counts = kafkaStreams.store(StoreQueryParameters.fromNameAndType
					("counts", QueryableStoreTypes.keyValueStore()));

			return counts.get(word);
		}
	}
}