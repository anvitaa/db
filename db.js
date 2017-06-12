
const _ = require('lodash')
const bunyan = require('bunyan')
const log = bunyan.createLogger({name: "yd-internal-api"})
const request = require('superagent');
const cheerio = require('cheerio')
const lda = require('lda')
const article_parser = require('article-parser')
const fetch = require('node-fetch')

const rake = require('node-rake')
log.info('connecting to db host:', process.env.DB_HOST)

const knex = require('knex')({
	client: 'mysql',
	connection: {
		host     : process.env.DB_HOST,
		user     : process.env.DB_USER,
		password : process.env.DB_PASS,
		database : process.env.DB_NAME
	}
})


// log.info([1,2,3])




var comments = getBoardCommentText()


function* lookupUserByToken(token) {
  return knex.select()
    .from('auth_tokens')
    .where('token', token)
}

function* getUserById(userId) {
  return knex.select()
    .from('users')
    .where('id', userId)
}

function* getBoardsFollowedByUser(userId) {
  return knex.select('title', 'board_id')
    .from('board_followers')
    .join('boards', 'boards.id', '=', 'board_followers.board_id')
    .where('board_followers.user_id', userId)
    .where('boards.deleted', 0)
}

function* getBoardStatsOwnedBy(userId) {
  let boards = yield getBoardsOwnedByUser(userId)
  let withStats = yield boards.map(b => {
    return addBoardStats(b, userId)
  })
  return withStats
}

// *note, this is the same query used for the board dropdown 
// a bit confusing b/c of the inner join
function* getBoardsOwnedOrFollowed(userId, orgId) {
  return knex.raw(`
    SELECT DISTINCT(b.id) as board_id,
    b.title, b.user_id as board_owner_id
    FROM boards b
    INNER JOIN organization_categories cat on b.organization_category_id = cat.id AND cat.deleted = 0
    LEFT JOIN board_followers f on b.id = f.board_id
    WHERE (b.user_id = ${userId} OR ( f.user_id = ${userId} AND f.status = 0))
    AND b.deleted = 0
    AND b.title != 'Private Bookmarks ${userId}'
    AND b.status in (1,2)
    AND b.organization_id = ${orgId}
    GROUP BY b.id
    ORDER by b.title`)
}

function* getBoardStatsOwnedOrAdminBy(userId, orgId) {
  let data = yield getBoardsOwnedOrFollowed(userId, orgId)
  data = data[0]
  let withStats = yield data.map(b => {
    return addBoardStats(b, userId)
  })
  return withStats
}

function* getBoardStatsFollowedBy(userId) {
  let boards = yield getBoardsFollowedByUser(userId)
  let withStats = yield boards.map(b => {
    return addBoardStats(b, userId)
  })
  return withStats
}

function* getBoardAdmins(boardId){
    return knex.select('user_id')
        .from('board_admins')
        .where('board_id', boardId)
        .where('status', 1)
}

function* addBoardStats(board, userId) {

  let points = yield getAllPointsOnBoard(board.board_id)

  let avg = _.sum(points.map(x => x.total)) / points.length

  let usersPoints = points.filter(x => x.user_id == userId) 

  let admins = yield getBoardAdmins(board.board_id)
    
  if (!usersPoints.length) {
    return _.merge(board, {
      board_admins: admins,
      users_points: 0, 
      followers: points.length,
      percentile: 0, 
      avg: Math.floor(avg)
    })
  }

  let position = points.map(x => x.user_id).indexOf(userId)
  let percentile = 100 - (position * 100 / points.length)

  return _.merge(board, {
    board_admins: admins,
    users_points: usersPoints[0].total,
    followers: points.length,
    percentile: Math.floor(percentile),
    avg: Math.floor(avg),

  })

}

function* getAllPointsOnBoard(boardId) {
  return knex('users_points')
    .distinct('user_id')
    .sum('points as total')
    .join('points_types', 'users_points.points_type_id', 'points_types.id')
    .where('board_id', boardId)
    .where('users_points.deleted', 0)
    .where('points_types.deleted', 0)
    .groupBy('user_id')
    .orderBy('total')
}

function* getBoardsOwnedByUser(userId) {
  return knex.select('title', 'id as board_id')
    .from('boards')
    .where('user_id', userId)
    .where('deleted', 0)
}

function* getBoardsAdminByUser(userId) {
  return knex.select('board_id')
    .from('board_admins')
    .orWhere('user_id', userId)
}

function* isBoardOwner(userId, boardId) {
  return knex.select()
    .from('boards')
    .where('user_id', userId)
    .where('id', boardId)
    .where('deleted', 0)
}

function* getBoardFollowers(boardId) {

  let subquery = knex.sum('points')
    .from('users_points')
    .where('users_points.user_id', knex.column('u.id'))
    .where('users_points.deleted', 0)
    .where('users_points.board_id', boardId)
    .as('points')

  var fields = [
    'u.id',
    'u.first_name',
    'u.last_name',
    'email',
    subquery
  ]

  return knex.select(fields)
    .from('users AS u')
    .join('board_followers', 'u.id', 'board_followers.user_id')
    .leftJoin('user_emails', 'user_emails.user_id', 'u.id')
    .where('board_followers.board_id', boardId)
    .where('primary', 1)
}

function* getPinIdsOnBoard(boardId) {
  return yield knex
    .select('id', 'user_id', 'created_at')
    .from('pins')
    .where('board_id', boardId)
    .where('deleted', 0)
    .map(x => {
      x.type = 'pin'
      return x
    })
}

function* getPinIdsOnBoardByUser(boardId, userId) {
  return yield knex
    .select('id', 'user_id', 'created_at')
    .from('pins')
    .where('board_id', boardId)
    .where('user_id', userId)
    .where('deleted', 0)
    .map(x => {
      x.type = 'pin'
      return x
    })
}


// *note 
// comments table doesnt' track board_id directly 
// but rather indirectly via pin_id
// thus why we pass in array of pins
function* getCommentIdsOnPins(pinIdsOnBoard) {
  return yield knex
    .select('id', 'user_id', 'pin_id', 'created_at')
    .from('comments')
    .where('deleted', 0)
    .whereIn('pin_id', pinIdsOnBoard.map(x => x.id))
    .map(x => {
      x.type = 'comment'
      return x
    })
}

function* getCommentIdsOnPinsByUser(pinIdsOnBoard, userId) {
  return yield knex
    .select('id', 'user_id', 'pin_id', 'created_at')
    .from('comments')
    .where('deleted', 0)
    .where('user_id', userId)
    .whereIn('pin_id', pinIdsOnBoard.map(x => x.id))
    .map(x => {
      x.type = 'comment'
      return x
    })
}

function* getPinVotesOnPins(pinIdsOnBoard) {
  return yield knex
    .select('id', 'user_id', 'pin_id')
    .from('pin_votes')
    .whereIn('pin_id', pinIdsOnBoard.map(x => x.id))
    .map(x => {
      x.type = 'vote'
      x.subtype = 'pin_vote'
      return x
    })
}

function* getCommentVotesOnPins(pinIdsOnBoard) {
  return yield knex
    .select('id', 'user_id', 'comment_id')
    .from('comment_votes')
    // TODO how are votes marked as deleted
    // .where('deleted', 0)
    .whereIn('pin_id', pinIdsOnBoard.map(x => x.id))
    .map(x => {
      x.type = 'vote'
      x.subtype = 'comment_vote'
      return x
    })
}

function* getInteractionsOnBoard(boardId) {
  let pinIdsOnBoard = yield getPinIdsOnBoard(boardId)
  let collections = yield [
    getCommentIdsOnPins(pinIdsOnBoard),
    getPinVotesOnPins(pinIdsOnBoard),
    getCommentVotesOnPins(pinIdsOnBoard),
    getBoardCommentText(pinIdsOnBoard)
  ]
  let together = Array.prototype.concat.apply([], collections.concat(pinIdsOnBoard))
  return together
}
function* getBoardCommentText(boardId) {

  let pinIdsOnBoard = yield getPinIdsOnBoard(boardId)
  let raw_comments = {}
  for(var i = 0; i < pinIdsOnBoard.length; i++) {
    var pin_id = pinIdsOnBoard[i].id
    let pin_comments = yield knex
      .select('body')
      .from('comments')
      .where('deleted', 0)
      .whereIn('pin_id', pin_id)
    if (pin_comments.length > 0) raw_comments[pin_id] = _.map(pin_comments, 'body')
  }
  return raw_comments
  
}






// old
function* getBoardContentText(boardId) {
  let pinIdsOnBoard = yield getPinIdsOnBoard(boardId)
  var promises = []; 



  //only get relevant text - parse html stuff out
  function parse(board_content) {
    board_content.text = cheerio.load(board_content.raw_html).text()
    // console.log('parse' + board_content.pin_id)
    return board_content
  }
  function text_anal(board_content) {
    console.log('text_anal' + board_content.article_content)
    board_content.lda_analysis = lda(board_content.article_content.match(/[^\.!\?]+[\.!\?]+/g), 2, 5)
    console.log(board_content.article_content)
     try {
      board_content.rake_analysis = rake.generate(board_content.article_content)
    } catch(err){
      console.log(board_content.pin_id+" "+err)
    }
    return board_content
    
  }
 
  
  //queries database to get pin information for all pins on a board
 
  for(var i = 0; i < pinIdsOnBoard.length; i++) {
    var pin_id = pinIdsOnBoard[i].id
    let pin_content = yield knex
      .select('title', 'user_note', 'body', 'url_web')
      .from('pins')
      .where('deleted', 0)
      .whereIn('id', pin_id)
    
    var board_content = pin_content[0]
    board_content.pin_id = pin_id

    //if there is a url provided, scrape its content
    if(board_content.url_web) {
      //scrape content from link 

      var content = new Promise(function(resolve, reject) {
        var x = board_content
        article_parser.extract(x.url_web).then((article) => {
          x.author = article.author
          x.source = article.source
          x.actual_title = article.title
          x.description = article.description
          fetch(x.url_web).then((article) => {
            return article.text()
          }).then((html) => {
              return [article_parser.parseMeta(html, x.url_web), article_parser.getArticle(html)]
              x.article_content = ;

              resolve(x)
          })


        })
      })

       

      // var parsed_text = content.then(parse)

      //nlp analysis on content

      var anal = content.then(text_anal)

      //return object 
      
      promises.push(anal)
    
    }
    else {
      promises.push(board_content)
    }

  }
  // console.log(promises)

  return Promise.all(promises).then(function(data_arr) {
    // console.log("uh oh")
    return data_arr
  })
}


module.exports = {
	getBoardStatsOwnedOrAdminBy,
  getAllPointsOnBoard,
  getBoardFollowers,
  getBoardStatsFollowedBy,
  getBoardStatsOwnedBy,
  getBoardsFollowedByUser,
  getBoardsOwnedByUser,
  getCommentIdsOnPins,
  getCommentIdsOnPinsByUser,
  getCommentVotesOnPins,
  getInteractionsOnBoard,
  getPinIdsOnBoard,
  getPinIdsOnBoardByUser,
  getPinVotesOnPins,
  getUserById,
  isBoardOwner,
  knex,
  getBoardCommentText,
  getBoardContentText,
  lookupUserByToken
}

