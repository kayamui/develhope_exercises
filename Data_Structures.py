list1 = ["lion", "monkey", "dog","fish"]
tuple1 = ("lion", "monkey", "dog","fish")
set1 = {"lion", "monkey", "dog","fish"}
dict1 = {"lion":4, "monkey":2, "dog":4,"fish":2}
# Question 1
print(len(list1), len(tuple1), len(set1), len(dict1) )
#Question 2
print(list1[0], tuple1[0])
#Question 3
print(dict1["lion"])
#Question 4
list1[1] = "rabbit"
print(list1)
#Question 5
#tuple1[1] = "rabbit" #It gives error because tuple objects are ummutable, cannot be changed, it is like you cant rip of a bulding from its place to another
#Question 6
list1.append("monkey")
print(list1)
#Question 7
list1.remove("rabbit")
print(list1)
#Question 8
dict1["fish"] = 0
print(dict1)
