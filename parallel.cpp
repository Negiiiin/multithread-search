
#include <iostream>
#include <string>
#include <vector>
#include <fstream>
#include <time.h>

#include <pthread.h>

#define NUMBER_OF_THREADS 2

using namespace std;

clock_t tStart = clock();
pthread_mutex_t mutex_;
pthread_mutex_t mutex_most_popular;

typedef struct Review {
    int book_id;
    int rating;
    int number_of_likes;
}review;

typedef struct Book {
    int book_id;
    string book_title;
    string genre_1;
    string genre_2;
    int pages;
    string author_name;
    float author_average_rating;
    int total_book_reviews_likes;
    vector<review> reviews;
}book;

vector<book> books;
vector<review> reviews;

float round(float var) {
    float value = (int)(var * 100 + .5); 
    return (float)value / 100; 
}

void make_new_files() {
    vector<string> lines;
    ifstream book_csv;
    book_csv.open("books.csv");
    while (book_csv.good())
    {
        string line;
        getline (book_csv,line);
        lines.push_back(line);
    }
    book_csv.close();
    int N = lines.size()/NUMBER_OF_THREADS;
    for (int i = 0; i < NUMBER_OF_THREADS-1; i++) {
        fstream fout; 
        string file_name = "Books_" + to_string(i) + ".csv"; 
        fout.open(file_name, ios::out | ios::app); 
        for (int j = N*i; j < N*(i+1); j++) {
            if (j + 1 == N*(i+1)) {
                fout << lines[j];
                continue;
            }
            fout << lines[j] << "\n";
        }
        fout.close();
    }
    fstream fout; 
    string file_name = "Books_" + to_string(NUMBER_OF_THREADS-1) + ".csv"; 
    fout.open(file_name, ios::out | ios::app); 
    for (int j = N*(NUMBER_OF_THREADS-1); j < lines.size(); j++) {
        if (j + 1 == lines.size()) {
            fout << lines[j];
            continue;
        }
        fout << lines[j] << "\n";
    }
    fout.close();
    lines.clear();
    ifstream review_csv;
    review_csv.open("reviews.csv");
    while (review_csv.good())
    {
        string line;
        getline (review_csv,line);
        lines.push_back(line);
    }
    review_csv.close();
    N = lines.size()/NUMBER_OF_THREADS;
    for (int i = 0; i < NUMBER_OF_THREADS-1; i++) {
        file_name = "Reviews_" + to_string(i) + ".csv"; 
        fout.open(file_name, ios::out | ios::app); 
        for (int j = N*i; j < N*(i+1); j++) {
            if (j + 1 == N*(i+1)) {
                fout << lines[j];
                continue;
            }
            fout << lines[j] << "\n";
        }
        fout.close();
    }
    file_name = "Reviews_" + to_string(NUMBER_OF_THREADS-1) + ".csv"; 
    fout.open(file_name, ios::out | ios::app); 
    for (int j = N*(NUMBER_OF_THREADS-1); j < lines.size(); j++) {
        if (j + 1 == lines.size()) {
            fout << lines[j];
            continue;
        }
        fout << lines[j] << "\n";
    }
    fout.close();
}

book omit_comma_book (string in){
    book new_book;
    string str = "";
    int place_of_comma = in.find(",");
    int palce_of_next_comma;
    for(int i = 0; i < place_of_comma; i++){
        str += in[i];
    }
    new_book.book_id = atoi(str.c_str());
    for(int j = 0; j < 6; j++) {
        string str = "";
        palce_of_next_comma = in.find(",", place_of_comma + 1);
        if(j == 5)
            palce_of_next_comma = in.size()-1;
        for(int i = place_of_comma+1; i < palce_of_next_comma; i++){
            str += in[i];
        }
        switch(j) {
            case 0:
                new_book.book_title = str;
            case 1:
                new_book.genre_1 = str;
            case 2:
                new_book.genre_2 = str;
            case 3:
                new_book.pages = atoi(str.c_str());
            case 4:
                new_book.author_name = str;
            case 5:
                new_book.author_average_rating = atof(str.c_str());
        }
        place_of_comma = palce_of_next_comma;
    }
    return new_book;
}

review omit_comma_review (string in) {
    review new_review;
    string str = "";
    int place_of_comma = in.find(",");
    int palce_of_next_comma = in.find(",", place_of_comma + 1);;
    for(int i = 0; i < place_of_comma; i++){
        str += in[i];
    }
    new_review.book_id = atoi(str.c_str());
    str = "";
    for(int i = place_of_comma + 1; i < palce_of_next_comma; i++){
        str += in[i];
    }
    new_review.rating = atoi(str.c_str());
    str = "";
    for(int i = palce_of_next_comma + 1; i < in.size(); i++){
        str += in[i];
    }
    new_review.number_of_likes = atoi(str.c_str());
    return new_review;
}

void* parse_data(void* tid) {
    long file_number = (long)tid;
    ifstream book_csv;
    book_csv.open("Books_" + to_string(file_number) + ".csv");
    pthread_mutex_lock (&mutex_);
    while (book_csv.good())
    {
        string line;
        getline (book_csv,line);
        books.push_back(omit_comma_book(line));
    }
    // pthread_mutex_unlock (&mutex_);
    book_csv.close();
    fstream reviews_csv;
    reviews_csv.open("Reviews_" + to_string(file_number) + ".csv"); 
    // pthread_mutex_lock (&mutex_);
    while (reviews_csv.good())
    {
        string line;
        string str;
        getline (reviews_csv,line);
        reviews.push_back(omit_comma_review(line));
    }
    pthread_mutex_unlock (&mutex_);
    reviews_csv.close();
    pthread_exit(NULL);
}

int partition (int book_or_review, int low, int high) {  
    if(book_or_review) {
        int pivot = reviews[high].book_id;
        int i = (low - 1);
        for (int j = low; j <= high - 1; j++)  
        {  
            if (reviews[j].book_id < pivot)  
            {  
                i++;
                review temp = reviews[j];  
                reviews[j] = reviews[i];  
                reviews[i] = temp;  
            }  
        }  
        review temp = reviews[i + 1];  
        reviews[i + 1] = reviews[high];  
        reviews[high] = temp;  
        return (i + 1);  
    }
    else {
        int pivot = books[high].book_id;
        int i = (low - 1);
        for (int j = low; j <= high - 1; j++)  
        {  
            if (books[j].book_id < pivot)  
            {  
                i++;
                book temp = books[j];  
                books[j] = books[i];  
                books[i] = temp;  
            }  
        }  
        book temp = books[i + 1];  
        books[i + 1] = books[high];  
        books[high] = temp;  
        return (i + 1);  
    }
} 

void quickSort (int book_or_review, int low, int high) {  
    if (low < high)  
    {  
        int pi = partition(book_or_review, low, high);  
        quickSort(book_or_review, low, pi - 1);  
        quickSort(book_or_review, pi + 1, high);  
    } 
}  

void find_reviews() {
    vector<review> book_reviews;
    int total_likes = 0;
    int reviews_counter = 0;
    for (int i = 0; i < books.size(); i++) {
        while (reviews[reviews_counter].book_id == books[i].book_id) {
            book_reviews.push_back(reviews[reviews_counter]);
            total_likes += reviews[reviews_counter].number_of_likes;
            reviews_counter ++;
        }
        books[i].reviews = book_reviews;
        books[i].total_book_reviews_likes = total_likes;
        book_reviews.clear();
        total_likes = 0;
    }
}

int find_max(vector<float> poppularities) {
    int result_index = 0;
    float max = -1.0;
    for (int i = 0; i < poppularities.size(); i++) {
        if (poppularities[i] > max) {
            max = poppularities[i];
            result_index = i;
        }
    }
    return result_index;
}

string genre;
vector <float> poppularities;
vector <int> indexes;


void* find_most_popular(void* tid) {
    int start_from = (long)tid;
    int N = books.size()/NUMBER_OF_THREADS;
    int limit;
    if (start_from == NUMBER_OF_THREADS-1) {
        limit = books.size();
    }
    else {
        limit = ((1+start_from)*N);
    }
    for (int i = start_from*N; i < limit; i++) {
        float popularity = books[i].author_average_rating;
        if (books[i].genre_1 == genre || books[i].genre_2 == genre) {
            if(books[i].total_book_reviews_likes == 0) {
                popularity = 0;
                continue;
            }
            float sigma = 0.0;
            for (int j = 0; j < books[i].reviews.size(); j++) {
                sigma += (books[i].reviews[j].rating * books[i].reviews[j].number_of_likes);
            }
            popularity += (sigma/books[i].total_book_reviews_likes);
            popularity *= 0.1;
        }
        else {
            continue;
        }
        pthread_mutex_lock (&mutex_);
        poppularities.push_back(popularity);
        indexes.push_back(i);
        pthread_mutex_unlock (&mutex_);

    }
    
    pthread_exit(NULL);
}

void print_result(int book_index) {
    cout << "id: " << books[indexes[book_index]].book_id << endl;
    cout << "Title: " << books[indexes[book_index]].book_title << endl;
    cout << "Genres: " << books[indexes[book_index]].genre_1 << ", " << books[indexes[book_index]].genre_2 << endl;
    cout << "Number of Pages: " << books[indexes[book_index]].pages << endl;
    cout << "Author: " << books[indexes[book_index]].author_name << endl;
    cout << "Average Rating: " << round(poppularities[book_index]) << endl;
}

void* QS_books(void* tid) {
    quickSort(0, 0, books.size()-1);
    pthread_exit(NULL);
}

void* QS_reviews(void* tid) {
    quickSort(1, 0, reviews.size()-1);
    pthread_exit(NULL);
}

int main(int argc, char const *argv[]){
    if (argc != 2){
        cout << "FAILED\n";
        exit(0);
    }
    genre = argv[1];
    void *status;
    // make_new_files();
    pthread_t threads[NUMBER_OF_THREADS];
    pthread_mutex_init(&mutex_, NULL);
    for(long tid = 0; tid < NUMBER_OF_THREADS; tid++)
	{
		pthread_create(&threads[tid], NULL, parse_data, (void*)tid);
	}
    for(long i = 0; i < NUMBER_OF_THREADS; i++)
		pthread_join(threads[i], &status);

	pthread_create(&threads[0], NULL, QS_books, NULL);
    pthread_create(&threads[1], NULL, QS_reviews, NULL);

    for(long i = 0; i < 2; i++)
		pthread_join(threads[i], &status);

    find_reviews();

    for(long tid = 0; tid < NUMBER_OF_THREADS; tid++)
	{
		pthread_create(&threads[tid], NULL, find_most_popular, (void*)tid);
	}
    for(long i = 0; i < NUMBER_OF_THREADS; i++)
		pthread_join(threads[i], &status);
    pthread_mutex_destroy(&mutex_);

    int book_index = find_max(poppularities);

    print_result(book_index);

    // cout << (double)(clock() - tStart)/CLOCKS_PER_SEC << endl;
    pthread_exit(NULL);
}
