package recfun

object Main {
  def main(args: Array[String]) {
    println("Pascal's Triangle")
    for (row <- 0 to 10) {
      for (col <- 0 to row)
        print(pascal(col, row) + " ")
      println()
    }
  }

  /**
   * Exercise 1
   */
    def pascal(c: Int, r: Int): Int = {
      if (r == 0 || c == 0  || c == r)
        1
      else if (r < 0 || c < 0 || c > r)
        0
      else
        pascal(c-1, r-1) +  pascal(c, r-1)
    }

  /**
   * Exercise 2
   */
    def balance(chars: List[Char]): Boolean = {
      def helper(chars: List[Char], balance: Int): Int = {
        if ( balance < 0 || chars.isEmpty) {
          balance
        }
        else
        {
          if (chars.head == '(' )
            helper(chars.tail, balance + 1)
          else if (chars.head == ')' )
            helper(chars.tail, balance - 1)
          else
            helper(chars.tail, balance )
        }
      }
      helper(chars, 0) == 0
    }
  
  /**
   * Exercise 3
   */
    def countChange(money: Int, coins: List[Int]): Int = {
      if (money == 0)
        1
      else if (coins.isEmpty || money < 0)
        0
      else
        countChange(money, coins.tail)+ countChange(money - coins.head, coins)
    }

}
