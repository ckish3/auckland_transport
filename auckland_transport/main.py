import download_data


def main():
    data = download_data.make_realtime_request()
    print(data.keys())
    return

    df = download_data.convert_request_to_df(data)
    print(df)


if __name__ == "__main__":
    main()