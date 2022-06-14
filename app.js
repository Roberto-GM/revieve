const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const fs = require('fs')

const files = ['customers', 'orders', 'products'];

Promise.all(
  files.map(file => {
    return new Promise((resolve, reject) => {
      const data = []
      fs.createReadStream(`./data/${file}.csv`)
        .pipe(csv())
        .on('data', row => data.push(row))
        .on('end', () => {
          resolve(data)
        })
    })
  })).then(values => {
    const customers = values[0];
    const orders = values[1];
    const products = values[2];

    task1(orders, products)
    task2(orders, products)
    task3(customers, orders, products)
  })


function task1(orders, products) {
  const result = orders.map(order => {
    return {
      id: order.id,
      euros: order.products.split(" ").map(elem => { return products.find(e => e.id === elem).cost }).reduce((a, b) => a + Number(b), 0)
    }
  })
  const csvWriter = createCsvWriter({
    path: 'order_prices.csv',
    header: [
      { id: 'id', title: 'id' },
      { id: 'euros', title: 'euros' }
    ]
  });

  csvWriter
    .writeRecords(result)
    .then(() => console.log('Task 1 done'))
}

function task2(orders, products) {
  const result = products.map(product => {
    return {
      id: product.id,
      customer_ids: orders.filter(order => order.products.split("").includes(product.id)).reduce((a, b) => a += `${b.customer} `, '').trimEnd()
    }
  }
  )
  const csvWriter = createCsvWriter({
    path: 'product_customers.csv',
    header: [
      { id: 'id', title: 'id' },
      { id: 'customer_ids', title: 'customer_ids' }
    ]
  });

  csvWriter
    .writeRecords(result)
    .then(() => console.log('Task 2 done'))
}

function task3(customers, orders, products) {
  const totalOrders = orders.map(order => {
    return {
      id: order.customer,
      euros: order.products.split(" ").map(elem => { return products.find(e => e.id === elem).cost }).reduce((a, b) => a + Number(b), 0)
    }
  })
  const result = customers.map(customer => {
    return {
      id: customer.id,
      firstname: customer.firstname,
      lastname: customer.lastname,
      total_euros: totalOrders.filter(order => order.id === customer.id).reduce((a, b) => a + Number(b.euros), 0)
    }
  }).sort( (a,b) => b.total_euros - a.total_euros );

  const csvWriter = createCsvWriter({
    path: 'customer_ranking.csv',
    header: [
      { id: 'id', title: 'id' },
      { id: 'firstname', title: 'firstname' },
      { id: 'lastname', title: 'lastname' },
      { id: 'total_euros', title: 'total_euros' }
    ]
  });

  csvWriter
    .writeRecords(result)
    .then(() => console.log('Task 3 done'))
}