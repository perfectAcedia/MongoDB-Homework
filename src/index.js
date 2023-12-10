import { connect, close } from "./connection.js";

const db = await connect();
const usersCollection = db.collection("users");
const articlesCollection = db.collection("articles");
const studentsCollection = db.collection("students");

const run = async () => {
  try {
    // await getUsersExample();
    // await task1();
    // await task2();
    // await task3();
    // await task4();
    // await task5();
    // await task6();
    // await task7();
    // await task8();
    // await task9();
    // await task10();
    // await task11();
    // await task12();

    await close();
  } catch (err) {
    console.log("Error: ", err);
  }
};
run();

// #### Users
// - Get users example
async function getUsersExample() {
  try {
    const [allUsers, firstUser] = await Promise.all([
      usersCollection.find().toArray(),
      usersCollection.findOne(),
    ]);

    console.log("allUsers", allUsers);
    console.log("firstUser", firstUser);
  } catch (err) {
    console.error("getUsersExample", err);
  }
}

// - Get all users, sort them by age (ascending), and return only 5 records with firstName, lastName, and age fields.
async function task1() {
  try {
    const result = await Promise.all([
      usersCollection
        .find({}, { projection: { firstName: 1, lastName: 1, age: 1, _id: 0 } })
        .sort({ age: 1 })
        .limit(5)
        .toArray(),
    ]);

    console.log("Task 1 Result:", result);
  } catch (err) {
    console.error("task1", err);
  }
}

// - Add new field 'skills: []" for all users where age >= 25 && age < 30 or tags includes 'Engineering'
async function task2() {
  try {
    const filter = {
      $or: [{ age: { $gte: 25, $lt: 30 } }, { tags: "Engineering" }],
    };

    const update = {
      $set: { skills: [] },
    };

    const result = await usersCollection.updateMany(filter, update);

    console.log("Task 2 Result:", result, "users updated");
  } catch (err) {
    console.error("task2", err);
  }
}

// - Update the first document and return the updated document in one operation (add 'js' and 'git' to the 'skills' array)
//   Filter: the document should contain the 'skills' field
async function task3() {
  try {
    const filter = { skills: { $exists: true, $ne: null } };

    const update = {
      $addToSet: { skills: { $each: ["js", "git"] } },
    };

    const options = { returnDocument: "after" };

    const updatedDocument = await usersCollection.findOneAndUpdate(
      filter,
      update,
      options
    );

    console.log("Task 3 Result:", updatedDocument);
  } catch (err) {
    console.error("task3", err);
  }
}

// - REPLACE the first document where the 'email' field starts with 'john' and the 'address state' is equal to 'CA'
//   Set firstName: "Jason", lastName: "Wood", tags: ['a', 'b', 'c'], department: 'Support'
async function task4() {
  try {
    const filter = {
      email: { $regex: /^john/i },
      "address.state": "CA",
    };

    const replacement = {
      firstName: "Jason",
      lastName: "Wood",
      tags: ["a", "b", "c"],
      department: "Support",
    };

    const options = { returnDocument: "before" };

    const originalDocument = await usersCollection.findOneAndReplace(
      filter,
      replacement,
      options
    );

    console.log("Task 4 Result:", originalDocument);
  } catch (err) {
    console.error("task4", err);
  }
}

// - Pull tag 'c' from the first document where firstName: "Jason", lastName: "Wood"
async function task5() {
  try {
    const filter = {
      firstName: "Jason",
      lastName: "Wood",
    };

    const update = {
      $pull: { tags: "c" },
    };

    const options = { returnDocument: "after" };

    const updatedDocument = await usersCollection.findOneAndUpdate(
      filter,
      update,
      options
    );

    console.log("Task 5 Result:", updatedDocument);
  } catch (err) {
    console.error("task5", err);
  }
}

// - Push tag 'b' to the first document where firstName: "Jason", lastName: "Wood"
//   ONLY if the 'b' value does not exist in the 'tags'
async function task6() {
  try {
    const filter = {
      firstName: "Jason",
      lastName: "Wood",
    };

    const update = {
      $addToSet: { tags: "b" },
    };

    const options = { returnDocument: "after" };

    const updatedDocument = await usersCollection.findOneAndUpdate(
      filter,
      update,
      options
    );

    console.log("Task 6 Result:", updatedDocument);
  } catch (err) {
    console.error("task6", err);
  }
}

// - Delete all users by department (Support)
async function task7() {
  try {
    const filter = {
      department: "Support",
    };

    const result = await usersCollection.deleteMany(filter);

    console.log("Task 7 Result:", result.deletedCount, "users deleted");
  } catch (err) {
    console.error("task7", err);
  }
}

// #### Articles
// - Create new collection 'articles'. Using bulk write:
//   Create one article per each type (a, b, c)
//   Find articles with type a, and update tag list with next value ['tag1-a', 'tag2-a', 'tag3']
//   Add tags ['tag2', 'tag3', 'super'] to articles except articles with type 'a'
//   Pull ['tag2', 'tag1-a'] from all articles
async function task8() {
  try {
    const articlesData = [{ type: "a" }, { type: "b" }, { type: "c" }];

    await articlesCollection.bulkWrite(
      articlesData.map((article) => ({
        insertOne: { document: article },
      }))
    );

    const updateTagForTypeA = {
      updateOne: {
        filter: { type: "a" },
        update: { $set: { tags: ["tag1-a", "tag2-a", "tag3"] } },
      },
    };

    const updateTagsForOthers = {
      updateMany: {
        filter: { type: { $ne: "a" } },
        update: { $set: { tags: ["tag2", "tag3", "super"] } },
      },
    };

    const pullTags = {
      updateMany: {
        filter: {},
        update: { $pull: { tags: { $in: ["tag2", "tag1-a"] } } },
      },
    };

    const result = await articlesCollection.bulkWrite([
      updateTagForTypeA,
      updateTagsForOthers,
      pullTags,
    ]);

    console.log("Task 8 Result:", result.modifiedCount, "articles updated");
  } catch (err) {
    console.error("task8", err);
  }
}

// - Find all articles that contains tags 'super' or 'tag2-a'
async function task9() {
  try {
    const filter = {
      tags: { $in: ["super", "tag2-a"] },
    };

    const result = await articlesCollection.find(filter).toArray();

    console.log("Task 9 Result:", result);
  } catch (err) {
    console.error("task9", err);
  }
}

// #### Students Statistic (Aggregations)
// - Find the student who have the worst score for homework, the result should be [ { name: <name>, worst_homework_score: <score> } ]
async function task10() {
  try {
    // Aggregation pipeline to find the student with the worst homework score
    const pipeline = [
      {
        $unwind: '$scores' // Split array into separate documents
      },
      {
        $match: { 'scores.type': 'homework' } // Filter to only include homework scores
      },
      {
        $group: {
          _id: '$_id',
          name: { $first: '$name' },
          worst_homework_score: { $first: '$scores.score' }
        }
      },
      {
        $sort: { 'worst_homework_score': 1 }
      },
      {
        $limit: 1
      }
    ];

    const result = await studentsCollection.aggregate(pipeline).toArray();

    console.log("Task 10 Result:", result);
  } catch (err) {
    console.error("task10", err);
  }
}


// - Calculate the average score for homework for all students, the result should be [ { avg_score: <number> } ]
async function task11() {
  try {
    const pipeline = [
      {
        $unwind: '$scores'
      },
      {
        $match: { 'scores.type': 'homework' }
      },
      {
        $group: {
          _id: null,
          avg_score: { $avg: '$scores.score' }
        }
      }
    ];

    const result = await studentsCollection.aggregate(pipeline).toArray();

    console.log("Task 11 Result:", result);
  } catch (err) {
    console.error("task11", err);
  }
}


// - Calculate the average score by all types (homework, exam, quiz) for each student, sort from the largest to the smallest value
async function task12() {
  try {
    const pipeline = [
      {
        $unwind: '$scores'
      },
      {
        $group: {
          _id: '$_id',
          name: { $first: '$name' },
          avg_scores: { $avg: '$scores.score' }
        }
      },
      {
        $sort: { avg_scores: -1 }
      }
    ];

    const result = await studentsCollection.aggregate(pipeline).toArray();

    console.log("Task 12 Result:", result);
  } catch (err) {
    console.error("task12", err);
  }
}

