package com.example.demo;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import com.example.demo.author.Author;
import com.example.demo.author.AuthorRepository;
import com.example.demo.book.Book;
import com.example.demo.book.BookRepository;

import connection.DataStaxAstraProperties;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterreadsPopulateDataApplication {

	@Autowired
	AuthorRepository authorRepository;
	@Autowired
	BookRepository bookRepository;

	@Value("${datadump.location.author}")
	private String autherDumpLocation;

	@Value("${datadump.location.works}")
	private String worksDumpLocation;

	public static void main(String[] args) {
		SpringApplication.run(BetterreadsPopulateDataApplication.class, args);
	}

	/**
	 * This is necessary to have the Spring Boot app use the Astra secure bundle to
	 * connect to the database
	 */
	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
		Path bundle = astraProperties.getSecureConnectBundle().toPath();
		return builder -> builder.withCloudSecureConnectBundle(bundle);
	}

	private void initAuthor() {
		Path path = Paths.get(autherDumpLocation);
		try (Stream<String> lines = Files.lines(path)) {
			lines.forEach(line -> {
				String jsonString = line.substring(line.indexOf("{"));
				JSONObject jsonObject = null;
				try {
					jsonObject = new JSONObject(jsonString);
				} catch (JSONException e) {
					e.printStackTrace();
				}

				Author author = new Author();
				author.setName(jsonObject.optString("name"));
				author.setPersonalName(jsonObject.optString("personal_name"));
				author.setId(jsonObject.optString("key").replace("/authors/", ""));
				System.out.println("name" + author.getName());
				authorRepository.save(author);
			});

		} catch (Exception e) {
			e.printStackTrace();

		}

	}

	private void initWorks() {
		Path path = Paths.get(worksDumpLocation);
		try (Stream<String> lines = Files.lines(path)) {
			lines.limit(10).forEach(line -> {
				String jsonString = line.substring(line.indexOf("{"));
				JSONObject jsonObject = null;
				DateTimeFormatter  dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
				
				try {
					jsonObject = new JSONObject(jsonString);
					Book book = new Book();
					book.setId(jsonObject.optString("title").replace("/works", ""));
					JSONArray authorJSONArray = jsonObject.optJSONArray("authors");
					if (authorJSONArray != null) {
						List<String> lst = new ArrayList<>();
						for (int i = 0; i < authorJSONArray.length(); i++) {
							String authorString = authorJSONArray.getJSONObject(i).getJSONObject("author")
									.optString("key");
							String authorId = authorString.replace("/authors/", "");
							lst.add(authorId);
						}
						book.setAuthorIds(lst);
						List<String> authorName = lst.stream().map(id -> authorRepository.findById(id))
								.map(optionalAuthor -> {
									if (!optionalAuthor.isPresent()) {
										return "Unkown Author";
									}
									return optionalAuthor.get().getName();

								}).collect(Collectors.toList());
						book.setAuthorNames(authorName);

					}

					JSONArray coverJSONArray = jsonObject.optJSONArray("covers");
					if (coverJSONArray != null) {
						List<String> lst = new ArrayList<>();
						for (int i = 0; i < coverJSONArray.length(); i++) {
							lst.add(coverJSONArray.getString(i));
						}
						book.setCoverIds(lst);
					}
					JSONObject jsonDescriptionObject=null;
					if(jsonObject.has("description")){

					jsonDescriptionObject = jsonObject.getJSONObject("description");
					if (jsonDescriptionObject != null) {
						book.setDescription(jsonDescriptionObject.optString("value"));
					}
					
					}
					book.setName(jsonObject.optString("title"));

					JSONObject jsonObjectPublishedDate = jsonObject.getJSONObject("created");
					if (jsonObjectPublishedDate != null) {
						book.setPublishedDate(LocalDate.parse(jsonObjectPublishedDate.optString("value"), dateFormatter));
					}
					System.out.println("book" + book.getName());
					bookRepository.save(book);

				} catch (JSONException e) {
					e.printStackTrace();
				}
				
			});
		} catch (Exception e) {
			e.printStackTrace();

		}

	}

	@PostConstruct
	public void start() {
		//initAuthor();
		initWorks();

	}

}
