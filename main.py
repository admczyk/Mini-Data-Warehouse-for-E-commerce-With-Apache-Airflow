from etl import extract, load, transform

def main():
    categories = ["products", "carts", "users"]
    # for category in categories:
    #     data = extract.fetch_data(category.upper())
    #     extract.save_as_file(data, category)
    transform.transform_product_data(transform.read_file(categories[0]))



if __name__ == "__main__":
    main()