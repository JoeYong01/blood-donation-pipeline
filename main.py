import requests

def download_file(url, filename):
    response = requests.get(url)
    if response.status_code == 200:
        with open(filename, 'wb') as file:
            file.write(response.content)
    else:
        print(f"Failed to download {filename}")

def main():
    file_urls = [
        'https://github.com/MoH-Malaysia/data-darah-public/blob/main/donations_facility.csv',
        "https://dub.sh/ds-data-granular"
        # Add more file URLs as needed
    ]
    for url in file_urls:
        filename = url.split('/')[-1]
        download_file(url, filename)

if __name__ == "__main__":
    main()

