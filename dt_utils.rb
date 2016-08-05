class String
  def insert_value
    "  #{'\''+self.strip+'\''},"
  end
end

class NilClass
  def insert_value
    'NULL,'
  end
end