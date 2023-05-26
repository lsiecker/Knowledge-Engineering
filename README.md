# Set-up
Create a new virtual environment (venv) for the project:

Open a terminal, navigate to your project's directory, and run the following command:

```
python3 -m venv ke_env
```
This will create a new virtual environment named ke_env in your project's directory.

Activate the virtual environment:

Depending on your operating system, the command to activate the virtual environment will differ.

On Windows:
```
ke_env\Scripts\activate
```
On Unix or MacOS:
```
source ke_env/bin/activate
```
You will know that you have the virtual environment activated when you see its name at the start of your command prompt, like this: `(ke_env) Your-Computer:your_project UserName$.`

While your virtual environment is activated, you can install the packages listed in your requirements.txt by running:
```
pip install -r requirements.txt
```

# Knowledge-Engineering
We as a client represent a team planning to create a fabulous movie. We definitely want to win an oscar and increase are chances of winning an oscar as much as possible. What should we focus on and change in our movie to make this possible? We have no plans on the movie yet so everything can be suggested even actors, producers, genre, release date, etc.

Research question:

Which features and feature combinations of a movie are most important for it to be nominated for an Oscar in the category best picture/film?

Additionally, which features and feature combinations of a movie are most important for it to win an Oscar in the category best picture/film?

datasets: 

1.

Dataset for all nominated and winning oscars movies of the last ~90 years: The Oscar Award, 1927 - 2023

https://datasetsearch.research.google.com/search?src=0&query=oscars&docid=L2cvMTFqOWJwX2hkcA%3D%3DLinks to an external site.

 

2. 

Datasets for finding information on movies (definitely advice to find as much data on the movies possible so use/find more movie data)

https://www.kaggle.com/datasets/disham993/9000-movies-datasetLinks to an external site.

https://www.kaggle.com/datasets/danielgrijalvas/moviesLinks to an external site.

 

PS. Could be interesting to look at datasets about specific actors, directors, or genres for even more specific data
