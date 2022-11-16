import json

if __name__ == '__main__':
    with open('movie.json', 'r', encoding='utf-8') as file:
        dict = json.load(file)
        chunk_len = (len(dict['movie']) // 8) + 1
        for i in range(8):
            out_file = open("./json_files/movie" + str(i + 1) + '.json', 'w')
            json.dump({'movie': dict['movie'][i * chunk_len: (i + 1) * chunk_len]}, out_file, indent=4)


