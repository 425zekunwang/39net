import os

import redis
import mmh3

class BloomFilter:
    def __init__(self, key, size, hash_count):
        self.redis_conn = redis.Redis(host='61.147.247.138', port=32012, db=0,password=os.getenv("RBQ_PASS"))
        self.key = key
        self.size = size
        self.hash_count = hash_count

    def add(self, item):
        for seed in range(self.hash_count):
            index = mmh3.hash(item, seed) % self.size
            self.redis_conn.setbit(self.key, index, 1)

    def __contains__(self, item):
        for seed in range(self.hash_count):
            index = mmh3.hash(item, seed) % self.size
            if not self.redis_conn.getbit(self.key, index):
                return False
        return True



if __name__ == "__main__":
    # Connect to Redis
    # Create a Bloom filter
    bloom_filter = BloomFilter('my_bloom_filter', 1000_000_00, 5)

    # Add some URLs to the bloom filter
    urls = ["http://example.com/page1", "http://example.com/page2", "http://example.com/page3"]
    for url in urls:
        bloom_filter.add(url)

    # Check if a URL is in the bloom filter
    url_to_check = "http://example.com/page1"
    if url_to_check in bloom_filter:
        print(f"{url_to_check} may exist (false positives possible)")
    else:
        print(f"{url_to_check} definitely does not exist")
