package org.community.bigdata.workshop.sparkintro.movielens.model;

import java.util.Date;

/**
 * Contains information about a movie, like id, title, release date, video release date, imdb url and genres
 * Created by tudor on 12/6/2015.
 */
public class Movie {
    private int id;
    private String movieTitle;
    private Date releaseDate;
    private Date videoReleaseDate;
    private String url;
    private String genres;

    public Movie(int id, String movieTitle, Date releaseDate, Date videoReleaseDate, String url, String genres) {
        this.id = id;
        this.movieTitle = movieTitle;
        this.releaseDate = releaseDate;
        this.videoReleaseDate = videoReleaseDate;
        this.url = url;
        this.genres = genres;
    }

    public int getId() {
        return id;
    }

    public String getMovieTitle() {
        return movieTitle;
    }

    public Date getReleaseDate() {
        return releaseDate;
    }

    public Date getVideoReleaseDate() {
        return videoReleaseDate;
    }

    public String getUrl() {
        return url;
    }

    public String getGenres() {
        return genres;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Movie movie = (Movie) o;

        if (id != movie.id) return false;
        if (movieTitle != null ? !movieTitle.equals(movie.movieTitle) : movie.movieTitle != null) return false;
        if (releaseDate != null ? !releaseDate.equals(movie.releaseDate) : movie.releaseDate != null) return false;
        if (videoReleaseDate != null ? !videoReleaseDate.equals(movie.videoReleaseDate) : movie.videoReleaseDate != null)
            return false;
        if (url != null ? !url.equals(movie.url) : movie.url != null) return false;
        return !(genres != null ? !genres.equals(movie.genres) : movie.genres != null);

    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (movieTitle != null ? movieTitle.hashCode() : 0);
        result = 31 * result + (releaseDate != null ? releaseDate.hashCode() : 0);
        result = 31 * result + (videoReleaseDate != null ? videoReleaseDate.hashCode() : 0);
        result = 31 * result + (url != null ? url.hashCode() : 0);
        result = 31 * result + (genres != null ? genres.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Movie{" +
                "id=" + id +
                ", movieTitle='" + movieTitle + '\'' +
                ", releaseDate=" + releaseDate +
                ", videoReleaseDate=" + videoReleaseDate +
                ", url='" + url + '\'' +
                ", genres='" + genres + '\'' +
                '}';
    }
}
