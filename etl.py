from libs import *


def extract_data_from_API(num_pages):
    headers = {
        "accept": "application/json",
    }

    with open("key.txt") as file: 
        headers['Authorization'] = file.readline()

    data = []

    for i in range(num_pages):
        url = f"https://api.themoviedb.org/3/movie/now_playing?language=en-US&page={i+1}"

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            response = response.json()
            data += response['results']   

        else:
            print(f"Request failed in iteration {i+1} with status code: {response.status_code}")
    
    return pd.DataFrame(data)