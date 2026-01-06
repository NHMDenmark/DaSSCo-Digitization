from pygbif import registry

# Replace 'Publisher Name' with the actual name of the publisher you're searching for
publisher_name = "Natural History Museum of Denmark"

# Use organization_suggest to search for the publisher
results = registry.organization_suggest(q=publisher_name)

# Display the results
if results:
    for publisher in results:
        print(f"Name: {publisher.get('title')}, UUID: {publisher.get('key')}")
else:
    print("No publishers found.")
