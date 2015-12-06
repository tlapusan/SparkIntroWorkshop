package org.community.bigdata.workshop.sparkintro.movielens.model;

/**
 * Created by tudor on 12/6/2015.
 */
public class Rating {
//    user id | item id | rating | timestamp
    private int userId;
    private int movieId;
    private float rating;
    private long timestamp;

    public Rating(int userId, int movieId, float rating, long timestamp) {
        this.userId = userId;
        this.movieId = movieId;
        this.rating = rating;
        this.timestamp = timestamp;
    }

    public int getUserId() {
        return userId;
    }

    public int getMovieId() {
        return movieId;
    }

    public float getRating() {
        return rating;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "Rating{" +
                "userId=" + userId +
                ", movieId=" + movieId +
                ", rating=" + rating +
                ", timestamp=" + timestamp +
                '}';
    }
}
