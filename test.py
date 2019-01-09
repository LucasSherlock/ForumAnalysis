import re


cols = ["Firefox", "Firefox Focus", "Firefox Lite", "Firefox OS", "Firefox Reality", "Firefox for Amazon Devices", "Firefox for Android", "Firefox for Enterprise", "Firefox for Fire TV", "Firefox for iOS", "Thunderbird", "Webmaker"]

print("Select a collection:\n")
i = 0
for string in cols:
    out = str(i) + " - " + cols[i] + ", "
    print(out)
    i += 1

num = -1

while num < 0 or num > i-1:
    num = int(input("Row Number: "))
    if num < 0 or num > i-1:
        print("Please enter a valid row number.")

print(cols[num])


